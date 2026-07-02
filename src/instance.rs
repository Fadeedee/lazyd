use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::data::FetchRange;
use crate::error::{Error, Result};
use crate::fanotify::FanotifyBackend;
use crate::range_map::{BITMAP_UNIT_BYTES, RangeMap, validate_fetch_unit_bytes};
use crate::remote::oci::OciRemoteBackend;
use crate::remote::{AuthConfig, BlobDescriptor, RemoteBackend, RemoteSource};

pub const DEFAULT_FETCH_UNIT_BYTES: u64 = BITMAP_UNIT_BYTES;
const PERSISTED_INSTANCE_VERSION: u32 = 1;
const PERSISTED_INSTANCE_FILE: &str = "instance.json";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerMode {
    #[default]
    Fanotify,
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceConfig {
    #[serde(skip)]
    pub instance_id: String,
    pub target_path: PathBuf,
    pub blob: BlobDescriptor,
    pub source: RemoteSource,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub fetch: FetchConfig,
    #[serde(default)]
    pub trigger_mode: TriggerMode,
}

impl InstanceConfig {
    fn describes_same_content(&self, other: &Self) -> bool {
        self.target_path == other.target_path
            && self.blob == other.blob
            && self.fetch == other.fetch
            && self.trigger_mode == other.trigger_mode
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedInstance {
    version: u32,
    instance_id: String,
    config: InstanceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchConfig {
    #[serde(default = "default_fetch_unit_bytes")]
    pub unit_bytes: u64,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            unit_bytes: DEFAULT_FETCH_UNIT_BYTES,
        }
    }
}

#[derive(Clone)]
pub struct InstanceRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    instances: RwLock<HashMap<String, Arc<Instance>>>,
    fanotify: Option<FanotifyBackend>,
}

pub struct Instance {
    config: InstanceConfig,
    target: std::fs::File,
    range_map: RangeMap,
    trusted: bool,
    remote: Arc<dyn RemoteBackend>,
    inflight: Mutex<Vec<Range>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Range {
    offset: u64,
    len: u64,
}

impl Range {
    fn end(self) -> Option<u64> {
        self.offset.checked_add(self.len)
    }

    fn overlaps(self, other: Self) -> bool {
        let Some(end) = self.end() else { return false };
        let Some(other_end) = other.end() else {
            return false;
        };
        self.offset < other_end && other.offset < end
    }
}

struct InflightGuard<'a> {
    range: Range,
    inflight: &'a Mutex<Vec<Range>>,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        let range = self.range;
        let inflight = self.inflight;
        let mut guard = inflight.lock().unwrap();
        guard.retain(|item| *item != range);
    }
}

impl InstanceRegistry {
    pub fn new(fanotify: Option<FanotifyBackend>) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                instances: RwLock::new(HashMap::new()),
                fanotify,
            }),
        }
    }

    pub async fn len(&self) -> usize {
        self.inner.instances.read().await.len()
    }

    pub async fn get(&self, instance_id: &str) -> Option<Arc<Instance>> {
        self.inner.instances.read().await.get(instance_id).cloned()
    }

    pub async fn register(&self, instance_id: String, config: InstanceConfig) -> Result<()> {
        self.register_inner(instance_id, config, false).await
    }

    pub async fn register_persistent(
        &self,
        instance_id: String,
        config: InstanceConfig,
    ) -> Result<()> {
        self.register_inner(instance_id, config, true).await
    }

    async fn register_inner(
        &self,
        instance_id: String,
        mut config: InstanceConfig,
        persist: bool,
    ) -> Result<()> {
        if config.target_path.as_os_str().is_empty() {
            return Err(Error::BadRequest("target_path is required".to_string()));
        }
        if config.blob.size == 0 {
            return Err(Error::BadRequest(
                "blob size must be greater than zero".to_string(),
            ));
        }
        validate_fetch_unit_bytes(config.fetch.unit_bytes)?;

        config.instance_id = instance_id.clone();
        let mut instances = self.inner.instances.write().await;
        if let Some(existing) = instances.get(&instance_id) {
            // Source and auth locate immutable content; they are not part of its identity.
            if !existing.config.describes_same_content(&config) {
                return Err(Error::Conflict(
                    "instance already exists with different config".to_string(),
                ));
            }
        }

        // Reopening a compatible instance refreshes its registry source/auth while
        // preserving the content identity and on-disk ready map.
        let instance = Arc::new(Instance::open(config.clone())?);
        if persist {
            persist_instance(&instance_id, &config)?;
        }
        if !instances.contains_key(&instance_id)
            && instance.config.trigger_mode == TriggerMode::Fanotify
        {
            if let Some(fanotify) = &self.inner.fanotify {
                fanotify.mark(instance_id.clone(), &instance.config.target_path)?;
            }
        }
        instances.insert(instance_id, instance);
        Ok(())
    }

    pub async fn restore_persisted(&self, cache_root: &Path) -> Result<usize> {
        if !cache_root.exists() {
            return Ok(0);
        }
        let mut state_paths = Vec::new();
        for entry in std::fs::read_dir(cache_root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let state_path = entry.path().join(PERSISTED_INSTANCE_FILE);
                if state_path.exists() {
                    state_paths.push(state_path);
                }
            }
        }
        state_paths.sort();

        let mut restored = 0;
        for state_path in state_paths {
            let bytes = std::fs::read(&state_path)?;
            let state: PersistedInstance = serde_json::from_slice(&bytes)?;
            validate_persisted_instance(&state_path, &state)?;
            self.register(state.instance_id, state.config).await?;
            restored += 1;
        }
        Ok(restored)
    }

    pub async fn unregister(&self, instance_id: &str) -> Result<()> {
        let removed = self.inner.instances.write().await.remove(instance_id);
        if let Some(instance) = removed {
            if instance.config.trigger_mode == TriggerMode::Fanotify {
                if let Some(fanotify) = &self.inner.fanotify {
                    fanotify.unmark(&instance.config.target_path)?;
                }
            }
            remove_persisted_instance(&instance.config.target_path)?;
        }
        Ok(())
    }
}

fn persisted_instance_path(target_path: &Path) -> Result<PathBuf> {
    let parent = target_path.parent().ok_or_else(|| {
        Error::BadRequest("instance target_path has no parent directory".to_string())
    })?;
    Ok(parent.join(PERSISTED_INSTANCE_FILE))
}

fn persist_instance(instance_id: &str, config: &InstanceConfig) -> Result<()> {
    let path = persisted_instance_path(&config.target_path)?;
    let parent = path.parent().ok_or_else(|| {
        Error::BadRequest("persisted instance path has no parent directory".to_string())
    })?;
    let tmp = parent.join(format!(".{PERSISTED_INSTANCE_FILE}.tmp"));
    let state = PersistedInstance {
        version: PERSISTED_INSTANCE_VERSION,
        instance_id: instance_id.to_string(),
        config: config.clone(),
    };
    let encoded = serde_json::to_vec_pretty(&state)?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    std::fs::rename(&tmp, &path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn remove_persisted_instance(target_path: &Path) -> Result<()> {
    let path = persisted_instance_path(target_path)?;
    if path.exists() {
        std::fs::remove_file(&path)?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
    }
    Ok(())
}

fn validate_persisted_instance(path: &Path, state: &PersistedInstance) -> Result<()> {
    if state.version != PERSISTED_INSTANCE_VERSION {
        return Err(Error::Conflict(format!(
            "persisted instance {} has unsupported version {}",
            path.display(),
            state.version
        )));
    }
    let cache_dir = path.parent().ok_or_else(|| {
        Error::BadRequest("persisted instance path has no parent directory".to_string())
    })?;
    let cache_key = crate::prepare::cache_key(&state.config.blob.digest)?;
    let expected_dir = cache_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::BadRequest("cache directory name is not UTF-8".to_string()))?;
    if expected_dir != cache_key
        || state.instance_id != crate::prepare::instance_id(&cache_key)
        || state.config.target_path != cache_dir.join("layer.erofs")
    {
        return Err(Error::Conflict(format!(
            "persisted instance {} does not match its cache directory",
            path.display()
        )));
    }
    RangeMap::validate_existing(
        &state.config.target_path,
        &state.config.blob,
        state.config.fetch.unit_bytes,
    )
}

impl Instance {
    fn open(config: InstanceConfig) -> Result<Self> {
        validate_fetch_unit_bytes(config.fetch.unit_bytes)?;
        let target = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&config.target_path)?;
        let opened =
            RangeMap::open_or_create(&config.target_path, &config.blob, config.fetch.unit_bytes)?;
        if opened.needs_recovery {
            opened.range_map.recovery_reconcile(&target)?;
        }
        let remote = Arc::new(OciRemoteBackend::from_config(
            &config.blob,
            &config.source,
            config.auth.clone(),
        )?) as Arc<dyn RemoteBackend>;
        Ok(Self::with_remote_and_range_map(
            config,
            target,
            opened.range_map,
            remote,
        ))
    }

    #[cfg(test)]
    fn with_mock_remote(
        mut config: InstanceConfig,
        target: std::fs::File,
        remote: Arc<dyn RemoteBackend>,
    ) -> Self {
        validate_fetch_unit_bytes(config.fetch.unit_bytes).unwrap();
        config.instance_id = "test".to_string();
        let opened =
            RangeMap::open_or_create(&config.target_path, &config.blob, config.fetch.unit_bytes)
                .unwrap();
        if opened.needs_recovery {
            opened.range_map.recovery_reconcile(&target).unwrap();
        }
        Self::with_remote_and_range_map(config, target, opened.range_map, remote)
    }

    fn with_remote_and_range_map(
        config: InstanceConfig,
        target: std::fs::File,
        range_map: RangeMap,
        remote: Arc<dyn RemoteBackend>,
    ) -> Self {
        Self {
            config,
            target,
            range_map,
            trusted: true,
            remote,
            inflight: Mutex::new(Vec::new()),
        }
    }

    pub async fn ensure_range(self: &Arc<Self>, offset: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        if end > self.config.blob.size {
            return Err(Error::BadRequest("range exceeds blob size".to_string()));
        }
        if !self.trusted {
            return Err(Error::Remote("instance is not trusted".to_string()));
        }
        if self.range_map.is_range_ready(offset, len) {
            return Ok(());
        }
        let range = self.amplify_to_fetch_unit(offset, len)?;
        let _guard = self.reserve_inflight(range).await?;
        if self.range_map.is_range_ready(offset, len) {
            return Ok(());
        }
        let bytes = self.remote.read_range(range.offset, range.len).await?;
        if bytes.len() != range.len as usize {
            return Err(Error::Remote(format!(
                "remote returned {} bytes, expected {}",
                bytes.len(),
                range.len
            )));
        }
        self.write_all_at(&bytes, range.offset)?;
        self.target.sync_data()?;
        self.range_map.set_range_ready(range.offset, range.len)?;
        Ok(())
    }

    pub async fn prepare_fetch_range(
        self: &Arc<Self>,
        offset: u64,
        len: u64,
        page_size: u64,
    ) -> Result<(FetchRange, File)> {
        if page_size == 0 {
            return Err(Error::BadRequest(
                "page size must be greater than zero".to_string(),
            ));
        }
        if len == 0 {
            return Err(Error::BadRequest(
                "fetch len must be greater than zero".to_string(),
            ));
        }
        if offset % page_size != 0 || len % page_size != 0 {
            return Err(Error::BadRequest(
                "fetch off and len must be page-aligned".to_string(),
            ));
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        let blob_size = self.config.blob.size;
        let blob_page_end = round_up(blob_size, page_size)?;
        if offset >= blob_size || end > blob_page_end {
            return Err(Error::BadRequest(
                "fetch range exceeds lazy blob tail page".to_string(),
            ));
        }

        let real_len = blob_size.min(end) - offset;
        self.ensure_range(offset, real_len).await?;
        let ready = self.amplify_to_fetch_unit(offset, real_len)?;
        let ready_end = round_up(
            ready
                .end()
                .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?,
            page_size,
        )?
        .min(blob_page_end);
        Ok((
            FetchRange {
                off: ready.offset,
                len: ready_end - ready.offset,
                dev_off: ready.offset,
            },
            self.target.try_clone()?,
        ))
    }

    fn amplify_to_fetch_unit(&self, offset: u64, len: u64) -> Result<Range> {
        let unit = self.config.fetch.unit_bytes;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        let fetch_offset = (offset / unit) * unit;
        let fetch_end = end.div_ceil(unit) * unit;
        let fetch_end = fetch_end.min(self.config.blob.size);
        Ok(Range {
            offset: fetch_offset,
            len: fetch_end - fetch_offset,
        })
    }

    async fn reserve_inflight(&self, range: Range) -> Result<InflightGuard<'_>> {
        loop {
            {
                let mut guard = self.inflight.lock().unwrap();
                if !guard.iter().any(|item| item.overlaps(range)) {
                    guard.push(range);
                    return Ok(InflightGuard {
                        range,
                        inflight: &self.inflight,
                    });
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    fn write_all_at(&self, bytes: &Bytes, mut offset: u64) -> Result<()> {
        let mut written = 0;
        while written < bytes.len() {
            let n = self.target.write_at(&bytes[written..], offset)?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write target_path",
                )));
            }
            written += n;
            offset += n as u64;
        }
        Ok(())
    }
}

fn round_up(value: u64, alignment: u64) -> Result<u64> {
    value
        .checked_add(alignment - 1)
        .map(|value| (value / alignment) * alignment)
        .ok_or_else(|| Error::BadRequest("range alignment overflows u64".to_string()))
}

fn default_fetch_unit_bytes() -> u64 {
    DEFAULT_FETCH_UNIT_BYTES
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use tempfile::NamedTempFile;

    use super::*;

    struct MockRemote {
        reads: AtomicUsize,
        delay: Duration,
        calls: Mutex<Vec<(u64, u64)>>,
    }

    #[async_trait]
    impl RemoteBackend for MockRemote {
        async fn read_range(&self, offset: u64, len: u64) -> Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.calls.lock().unwrap().push((offset, len));
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(Bytes::from(vec![offset as u8; len as usize]))
        }
    }

    fn mock_remote() -> Arc<MockRemote> {
        Arc::new(MockRemote {
            reads: AtomicUsize::new(0),
            delay: Duration::ZERO,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn config(path: PathBuf) -> InstanceConfig {
        InstanceConfig {
            instance_id: String::new(),
            target_path: path,
            blob: BlobDescriptor {
                digest: "sha256:abc".to_string(),
                size: 128,
                media_type: None,
            },
            source: RemoteSource::OciRegistry {
                image_ref: "registry.example.com/ns/image:tag".to_string(),
                hosts_dir: None,
            },
            auth: None,
            fetch: FetchConfig::default(),
            trigger_mode: TriggerMode::Fanotify,
        }
    }

    #[tokio::test]
    async fn registry_registers_idempotently_and_rejects_conflict() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        let registry = InstanceRegistry::new(None);
        let config = config(file.path().to_path_buf());

        registry
            .register("one".to_string(), config.clone())
            .await
            .unwrap();
        registry
            .register("one".to_string(), config.clone())
            .await
            .unwrap();
        assert_eq!(registry.len().await, 1);

        let mut conflict = config;
        conflict.blob.size = 64;
        assert!(matches!(
            registry.register("one".to_string(), conflict).await,
            Err(Error::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn compatible_registration_refreshes_remote_source() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        let registry = InstanceRegistry::new(None);
        let first = config(file.path().to_path_buf());
        registry
            .register("one".to_string(), first.clone())
            .await
            .unwrap();

        let mut refreshed = first;
        refreshed.source = RemoteSource::OciRegistry {
            image_ref: "registry.example.com/other/image:tag".to_string(),
            hosts_dir: Some("/etc/containerd/certs.d".to_string()),
        };
        registry
            .register("one".to_string(), refreshed.clone())
            .await
            .unwrap();

        assert_eq!(
            registry.get("one").await.unwrap().config.source,
            refreshed.source
        );
    }

    #[tokio::test]
    async fn persistent_instance_is_restored_after_registry_restart() {
        let root = tempfile::tempdir().unwrap();
        let cache_key = "sha256-1111111111111111111111111111111111111111111111111111111111111111";
        let cache_dir = root.path().join(cache_key);
        std::fs::create_dir_all(&cache_dir).unwrap();
        let target = cache_dir.join("layer.erofs");
        File::create(&target).unwrap().set_len(128).unwrap();
        let mut persistent = config(target);
        persistent.blob.digest =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111".to_string();
        let instance_id = format!("erofs-{cache_key}");

        let registry = InstanceRegistry::new(None);
        registry
            .register_persistent(instance_id.clone(), persistent.clone())
            .await
            .unwrap();
        drop(registry);

        let restored = InstanceRegistry::new(None);
        assert_eq!(restored.restore_persisted(root.path()).await.unwrap(), 1);
        let instance = restored.get(&instance_id).await.unwrap();
        assert_eq!(instance.config.source, persistent.source);
        assert_eq!(instance.config.auth, persistent.auth);
    }

    #[tokio::test]
    async fn registry_unregister_missing_instance_is_idempotent() {
        let registry = InstanceRegistry::new(None);
        registry.unregister("missing").await.unwrap();
        assert_eq!(registry.len().await, 0);
    }

    #[tokio::test]
    async fn missing_range_is_written_to_target_path() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        let remote = mock_remote();
        let instance = Arc::new(Instance::with_mock_remote(
            config(file.path().to_path_buf()),
            file.reopen().unwrap(),
            remote.clone(),
        ));

        instance.ensure_range(8, 4).await.unwrap();

        let mut buf = [0; 4];
        file.as_file().read_at(&mut buf, 8).unwrap();
        assert_eq!(buf, [0, 0, 0, 0]);
        assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn missing_range_is_amplified_to_fetch_unit() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(2 * 1024 * 1024).unwrap();
        let remote = mock_remote();
        let mut cfg = config(file.path().to_path_buf());
        cfg.blob.size = 2 * 1024 * 1024;
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote.clone(),
        ));

        instance.ensure_range(8 * 1024, 4 * 1024).await.unwrap();

        assert_eq!(remote.calls.lock().unwrap().as_slice(), &[(0, 1024 * 1024)]);
        assert!(instance.range_map.is_range_ready(8 * 1024, 4 * 1024));
    }

    #[tokio::test]
    async fn fetch_returns_complete_page_aligned_ready_unit() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(2 * 1024 * 1024).unwrap();
        let remote = mock_remote();
        let mut cfg = config(file.path().to_path_buf());
        cfg.blob.size = 2 * 1024 * 1024;
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote.clone(),
        ));

        let (range, _fd) = instance
            .prepare_fetch_range(8 * 1024, 4 * 1024, 4 * 1024)
            .await
            .unwrap();

        assert_eq!(remote.calls.lock().unwrap().as_slice(), &[(0, 1024 * 1024)]);
        assert_eq!(
            range,
            FetchRange {
                off: 0,
                len: 1024 * 1024,
                dev_off: 0,
            }
        );
    }

    #[tokio::test]
    async fn fetch_unit_can_span_multiple_bitmap_slots() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(4 * 1024 * 1024).unwrap();
        let remote = mock_remote();
        let mut cfg = config(file.path().to_path_buf());
        cfg.blob.size = 4 * 1024 * 1024;
        cfg.fetch.unit_bytes = 2 * 1024 * 1024;
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote.clone(),
        ));

        instance.ensure_range(8 * 1024, 4 * 1024).await.unwrap();

        assert_eq!(
            remote.calls.lock().unwrap().as_slice(),
            &[(0, 2 * 1024 * 1024)]
        );
        assert!(instance.range_map.is_range_ready(0, 1024 * 1024));
        assert!(instance.range_map.is_range_ready(1024 * 1024, 1024 * 1024));
        assert!(!instance.range_map.is_range_ready(2 * 1024 * 1024, 1));

        instance
            .ensure_range(1024 * 1024 + 8 * 1024, 4 * 1024)
            .await
            .unwrap();
        assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fetch_unit_must_align_to_bitmap_unit() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(2 * 1024 * 1024).unwrap();
        let registry = InstanceRegistry::new(None);
        let mut small_fetch = config(file.path().to_path_buf());
        small_fetch.blob.size = 2 * 1024 * 1024;
        small_fetch.fetch.unit_bytes = 512 * 1024;

        assert!(matches!(
            registry.register("one".to_string(), small_fetch).await,
            Err(Error::BadRequest(_))
        ));

        let mut unaligned_fetch = config(file.path().to_path_buf());
        unaligned_fetch.blob.size = 2 * 1024 * 1024;
        unaligned_fetch.fetch.unit_bytes = 1024 * 1024 + 512 * 1024;

        assert!(matches!(
            registry.register("two".to_string(), unaligned_fetch).await,
            Err(Error::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn present_range_does_not_read_remote() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        file.as_file().write_all_at(b"xxxx", 16).unwrap();
        let remote = mock_remote();
        let instance = Arc::new(Instance::with_mock_remote(
            config(file.path().to_path_buf()),
            file.reopen().unwrap(),
            remote.clone(),
        ));
        instance.range_map.set_range_ready(16, 4).unwrap();

        instance.ensure_range(16, 4).await.unwrap();

        assert_eq!(remote.reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn range_must_not_exceed_blob_size() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        let remote = mock_remote();
        let instance = Arc::new(Instance::with_mock_remote(
            config(file.path().to_path_buf()),
            file.reopen().unwrap(),
            remote,
        ));

        assert!(matches!(
            instance.ensure_range(120, 16).await,
            Err(Error::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn overlapping_inflight_range_is_deduplicated() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        let remote = Arc::new(MockRemote {
            reads: AtomicUsize::new(0),
            delay: Duration::from_millis(25),
            calls: Mutex::new(Vec::new()),
        });
        let instance = Arc::new(Instance::with_mock_remote(
            config(file.path().to_path_buf()),
            file.reopen().unwrap(),
            remote.clone(),
        ));

        let first = {
            let instance = instance.clone();
            tokio::spawn(async move { instance.ensure_range(32, 8).await })
        };
        let second = {
            let instance = instance.clone();
            tokio::spawn(async move { instance.ensure_range(32, 8).await })
        };

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
    }
}
