use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::fanotify::FanotifyBackend;
use crate::range_map::{BITMAP_UNIT_BYTES, RangeMap, validate_fetch_unit_bytes};
use crate::remote::oci::OciRemoteBackend;
use crate::remote::{AuthConfig, BlobDescriptor, RemoteBackend, RemoteSource};

pub const DEFAULT_FETCH_UNIT_BYTES: u64 = BITMAP_UNIT_BYTES;

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

    pub async fn register(&self, instance_id: String, mut config: InstanceConfig) -> Result<()> {
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
            if existing.config == config {
                return Ok(());
            }
            return Err(Error::Conflict(
                "instance already exists with different config".to_string(),
            ));
        }

        let instance = Arc::new(Instance::open(config)?);
        if let Some(fanotify) = &self.inner.fanotify {
            fanotify.mark(instance_id.clone(), &instance.config.target_path)?;
        }
        instances.insert(instance_id, instance);
        Ok(())
    }

    pub async fn unregister(&self, instance_id: &str) -> Result<()> {
        let removed = self.inner.instances.write().await.remove(instance_id);
        if let (Some(instance), Some(fanotify)) = (removed, &self.inner.fanotify) {
            fanotify.unmark(&instance.config.target_path)?;
        }
        Ok(())
    }
}

impl Instance {
    fn open(config: InstanceConfig) -> Result<Self> {
        validate_fetch_unit_bytes(config.fetch.unit_bytes)?;
        let target = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&config.target_path)?;
        let opened = RangeMap::open_or_create(&config.target_path, &config.blob)?;
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
        let opened = RangeMap::open_or_create(&config.target_path, &config.blob).unwrap();
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
        self.range_map.set_range_ready(range.offset, range.len)?;
        Ok(())
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
