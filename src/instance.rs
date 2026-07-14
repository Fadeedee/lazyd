use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::data::FetchRange;
use crate::error::{Error, Result};
use crate::extent_layout::DataExtent;
use crate::extent_map::ExtentMap;
use crate::fanotify::FanotifyBackend;
use crate::range_map::{BITMAP_UNIT_BYTES, RangeMap, validate_fetch_unit_bytes};
use crate::remote::kuasar::KuasarRemoteBackend;
use crate::remote::oci::OciRemoteBackend;
use crate::remote::{AuthConfig, BlobDescriptor, RemoteBackend, RemoteRange, RemoteSource};

pub const DEFAULT_FETCH_UNIT_BYTES: u64 = BITMAP_UNIT_BYTES;

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
    #[serde(skip)]
    pub canonical_extents: Option<Arc<[DataExtent]>>,
    #[serde(default)]
    pub trigger_mode: TriggerMode,
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
    range_map: InstanceRangeMap,
    trusted: bool,
    remote: Arc<dyn RemoteBackend>,
    inflight: Mutex<Vec<Range>>,
}

enum InstanceRangeMap {
    Fixed(RangeMap),
    Extent(ExtentMap),
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
        match (&config.source, &config.canonical_extents) {
            (RemoteSource::KuasarManifest { .. }, None) => {
                return Err(Error::BadRequest(
                    "canonical_extents is required for a kuasar-manifest instance".to_string(),
                ));
            }
            (RemoteSource::OciRegistry { .. }, Some(_)) => {
                return Err(Error::BadRequest(
                    "canonical_extents is only valid for a kuasar-manifest instance".to_string(),
                ));
            }
            _ => {}
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
        if instance.config.trigger_mode == TriggerMode::Fanotify {
            if let Some(fanotify) = &self.inner.fanotify {
                fanotify.mark(instance_id.clone(), &instance.config.target_path)?;
            }
        }
        instances.insert(instance_id, instance);
        Ok(())
    }

    pub async fn unregister(&self, instance_id: &str) -> Result<()> {
        let removed = self.inner.instances.write().await.remove(instance_id);
        if let (Some(instance), Some(fanotify)) = (removed, &self.inner.fanotify) {
            if instance.config.trigger_mode == TriggerMode::Fanotify {
                fanotify.unmark(&instance.config.target_path)?;
            }
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
        let range_map = match &config.source {
            RemoteSource::KuasarManifest { .. } => {
                let extents = config.canonical_extents.clone().ok_or_else(|| {
                    Error::BadRequest(
                        "canonical_extents is required for a kuasar-manifest instance".to_string(),
                    )
                })?;
                let opened = ExtentMap::open_or_create(&config.target_path, &config.blob, extents)?;
                if opened.needs_recovery {
                    opened.extent_map.recovery_reconcile(&target)?;
                }
                InstanceRangeMap::Extent(opened.extent_map)
            }
            RemoteSource::OciRegistry { .. } => {
                let opened = RangeMap::open_or_create(
                    &config.target_path,
                    &config.blob,
                    config.fetch.unit_bytes,
                )?;
                if opened.needs_recovery {
                    opened.range_map.recovery_reconcile(&target)?;
                }
                InstanceRangeMap::Fixed(opened.range_map)
            }
        };
        let remote = match &config.source {
            RemoteSource::OciRegistry { .. } => Arc::new(OciRemoteBackend::from_config(
                &config.blob,
                &config.source,
                config.auth.clone(),
            )?) as Arc<dyn RemoteBackend>,
            RemoteSource::KuasarManifest { .. } => {
                Arc::new(KuasarRemoteBackend::from_source(&config.source)?)
                    as Arc<dyn RemoteBackend>
            }
        };
        Ok(Self::with_remote_and_range_map(
            config, target, range_map, remote,
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
        let range_map = if matches!(&config.source, RemoteSource::KuasarManifest { .. }) {
            let extents = config
                .canonical_extents
                .clone()
                .expect("Kuasar test instance must include canonical_extents");
            let opened =
                ExtentMap::open_or_create(&config.target_path, &config.blob, extents).unwrap();
            if opened.needs_recovery {
                opened.extent_map.recovery_reconcile(&target).unwrap();
            }
            InstanceRangeMap::Extent(opened.extent_map)
        } else {
            let opened = RangeMap::open_or_create(
                &config.target_path,
                &config.blob,
                config.fetch.unit_bytes,
            )
            .unwrap();
            if opened.needs_recovery {
                opened.range_map.recovery_reconcile(&target).unwrap();
            }
            InstanceRangeMap::Fixed(opened.range_map)
        };
        Self::with_remote_and_range_map(config, target, range_map, remote)
    }

    fn with_remote_and_range_map(
        config: InstanceConfig,
        target: std::fs::File,
        range_map: InstanceRangeMap,
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
        self.ensure_range_inner(offset, len).await
    }

    async fn ensure_range_inner(&self, offset: u64, len: u64) -> Result<()> {
        match &self.range_map {
            InstanceRangeMap::Fixed(range_map) => {
                self.ensure_fixed_range(range_map, offset, len).await
            }
            InstanceRangeMap::Extent(range_map) => {
                self.ensure_extent_range(range_map, offset, len).await
            }
        }
    }

    async fn ensure_fixed_range(&self, range_map: &RangeMap, offset: u64, len: u64) -> Result<()> {
        if range_map.is_range_ready(offset, len) {
            return Ok(());
        }
        let range = self.amplify_to_fetch_unit(offset, len)?;
        let _guard = self.reserve_inflight(range).await?;
        if range_map.is_range_ready(offset, len) {
            return Ok(());
        }
        let remote_range = self.remote.read_range(range.offset, range.len).await?;
        if remote_range.is_empty() {
            return Err(Error::Remote("remote returned an empty range".to_string()));
        }
        if remote_range.len() != range.len {
            return Err(Error::Remote(format!(
                "remote returned {} bytes, expected {}",
                remote_range.len(),
                range.len
            )));
        }
        self.write_remote_range(remote_range, range.offset)?;
        self.target.sync_data()?;
        range_map.set_range_ready(range.offset, range.len)?;
        Ok(())
    }

    async fn ensure_extent_range(
        &self,
        range_map: &ExtentMap,
        offset: u64,
        len: u64,
    ) -> Result<()> {
        if range_map.is_range_ready(offset, len)? {
            return Ok(());
        }
        let missing = range_map.missing_extents(offset, len)?;
        for (index, extent) in missing {
            let range = Range {
                offset: extent.offset,
                len: extent.len,
            };
            let _guard = self.reserve_inflight(range).await?;
            if range_map.is_range_ready(extent.offset, extent.len)? {
                continue;
            }
            let remote_range = self.remote.read_range(extent.offset, extent.len).await?;
            if remote_range.is_empty() {
                return Err(Error::Remote("remote returned an empty range".to_string()));
            }
            if remote_range.len() != extent.len {
                return Err(Error::Remote(format!(
                    "remote returned {} bytes, expected {}",
                    remote_range.len(),
                    extent.len
                )));
            }
            self.write_remote_range(remote_range, extent.offset)?;
            self.target.sync_data()?;
            range_map.set_extent_ready(index)?;
        }
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
        let blob_page_end = blob_size.div_ceil(page_size) * page_size;
        if offset >= blob_size || end > blob_page_end {
            return Err(Error::BadRequest(
                "fetch range exceeds lazy blob tail page".to_string(),
            ));
        }

        let real_len = blob_size.min(end) - offset;
        self.ensure_range(offset, real_len).await?;
        Ok((
            FetchRange {
                off: offset,
                len,
                dev_off: offset,
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

    fn write_remote_range(&self, range: RemoteRange, offset: u64) -> Result<()> {
        match range {
            RemoteRange::Bytes(bytes) => self.write_all_at(&bytes, offset),
            staging @ RemoteRange::StagingFile { .. } => {
                let (file, len) = staging.into_staging_file().ok_or_else(|| {
                    Error::Remote("remote staging range lost its file".to_string())
                })?;
                copy_file_range_or_fallback(&file, &self.target, offset, len)
            }
        }
    }
}

fn copy_file_range_or_fallback(
    source: &File,
    target: &File,
    target_offset: u64,
    len: u64,
) -> Result<()> {
    let mut source_offset: libc::loff_t = 0;
    let mut target_offset: libc::loff_t = target_offset
        .try_into()
        .map_err(|_| Error::BadRequest("target offset exceeds loff_t".to_string()))?;
    let mut copied = 0u64;
    while copied < len {
        let chunk = usize::try_from((len - copied).min(16 * 1024 * 1024))
            .map_err(|_| Error::BadRequest("copy length exceeds usize".to_string()))?;
        let result = unsafe {
            libc::copy_file_range(
                source.as_raw_fd(),
                &mut source_offset,
                target.as_raw_fd(),
                &mut target_offset,
                chunk,
                0,
            )
        };
        if result > 0 {
            copied += result as u64;
            continue;
        }
        if result == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "staging file ended before the requested range",
            )));
        }

        let err = std::io::Error::last_os_error();
        if matches!(
            err.raw_os_error(),
            Some(libc::EXDEV)
                | Some(libc::EINVAL)
                | Some(libc::ENOSYS)
                | Some(libc::EOPNOTSUPP)
                | Some(libc::EPERM)
        ) {
            break;
        }
        return Err(err.into());
    }

    copy_file_at(source, target, copied, target_offset as u64, len - copied)
}

fn copy_file_at(
    source: &File,
    target: &File,
    mut source_offset: u64,
    mut target_offset: u64,
    mut len: u64,
) -> Result<()> {
    let mut buffer = vec![0; 128 * 1024];
    while len > 0 {
        let chunk = usize::try_from(len.min(buffer.len() as u64))
            .map_err(|_| Error::BadRequest("copy length exceeds usize".to_string()))?;
        let read = source.read_at(&mut buffer[..chunk], source_offset)?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "staging file ended before the requested range",
            )));
        }

        let mut written = 0;
        while written < read {
            let count = target.write_at(&buffer[written..read], target_offset)?;
            if count == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write staging data to cache",
                )));
            }
            written += count;
            target_offset += count as u64;
        }
        source_offset += read as u64;
        len -= read as u64;
    }
    Ok(())
}

fn default_fetch_unit_bytes() -> u64 {
    DEFAULT_FETCH_UNIT_BYTES
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
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

    struct StagingRemote {
        data: Vec<u8>,
    }

    #[async_trait]
    impl RemoteBackend for StagingRemote {
        async fn read_range(&self, offset: u64, len: u64) -> Result<RemoteRange> {
            let mut file = tempfile::tempfile().unwrap();
            let start = offset as usize;
            let end = start + len as usize;
            file.write_all(&self.data[start..end]).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            Ok(RemoteRange::StagingFile { file, len })
        }
    }

    #[async_trait]
    impl RemoteBackend for MockRemote {
        async fn read_range(&self, offset: u64, len: u64) -> Result<RemoteRange> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.calls.lock().unwrap().push((offset, len));
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(RemoteRange::Bytes(Bytes::from(vec![
                offset as u8;
                len as usize
            ])))
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
            canonical_extents: None,
            trigger_mode: TriggerMode::Fanotify,
        }
    }

    fn kuasar_config(path: PathBuf, size: u64, extents: Vec<DataExtent>) -> InstanceConfig {
        InstanceConfig {
            instance_id: String::new(),
            target_path: path,
            blob: BlobDescriptor {
                digest: "sha256:kuasar".to_string(),
                size,
                media_type: None,
            },
            source: RemoteSource::KuasarManifest {
                manifest_keys: vec!["11".repeat(32)],
                accelerator_socket: "/run/accelerator/manifest-range.sock".to_string(),
            },
            auth: None,
            fetch: FetchConfig::default(),
            canonical_extents: Some(extents.into()),
            trigger_mode: TriggerMode::External,
        }
    }

    fn range_is_ready(instance: &Instance, offset: u64, len: u64) -> bool {
        match &instance.range_map {
            InstanceRangeMap::Fixed(range_map) => range_map.is_range_ready(offset, len),
            InstanceRangeMap::Extent(range_map) => range_map.is_range_ready(offset, len).unwrap(),
        }
    }

    fn mark_fixed_range_ready(instance: &Instance, offset: u64, len: u64) {
        let InstanceRangeMap::Fixed(range_map) = &instance.range_map else {
            panic!("test instance does not use a fixed range map");
        };
        range_map.set_range_ready(offset, len).unwrap();
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
    async fn registry_rejects_caller_authored_kuasar_layout() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(4096).unwrap();
        let config: InstanceConfig = serde_json::from_value(serde_json::json!({
            "target_path": file.path(),
            "blob": {
                "digest": "sha256:kuasar",
                "size": 4096
            },
            "source": {
                "type": "kuasar-manifest",
                "manifest_keys": ["11".repeat(32)],
                "accelerator_socket": "/run/accelerator/manifest-range.sock"
            },
            "canonical_extents": [],
            "trigger_mode": "external"
        }))
        .unwrap();
        let registry = InstanceRegistry::new(None);

        let error = registry
            .register("erofs-kuasar".to_string(), config)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("canonical_extents"), "{error}");
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
    async fn staging_file_is_copied_to_cache_and_tail_stays_zero() {
        let blob_size = 4097;
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(8192).unwrap();
        let remote = Arc::new(StagingRemote {
            data: vec![b'k'; blob_size],
        });
        let mut cfg = config(file.path().to_path_buf());
        cfg.blob.size = blob_size as u64;
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote,
        ));

        instance.ensure_range(4096, 1).await.unwrap();

        let mut data = vec![0; 8192];
        file.as_file().read_exact_at(&mut data, 0).unwrap();
        assert_eq!(&data[..blob_size], vec![b'k'; blob_size]);
        assert!(data[blob_size..].iter().all(|byte| *byte == 0));
        assert!(range_is_ready(&instance, 0, blob_size as u64));
    }

    #[test]
    fn pread_pwrite_fallback_preserves_source_and_target_offsets() {
        let mut source = tempfile::tempfile().unwrap();
        source.write_all(b"0123456789").unwrap();
        let target = tempfile::tempfile().unwrap();
        target.set_len(16).unwrap();

        copy_file_at(&source, &target, 2, 5, 4).unwrap();

        let mut data = [0; 16];
        target.read_exact_at(&mut data, 0).unwrap();
        assert_eq!(&data[5..9], b"2345");
        assert!(data[..5].iter().all(|byte| *byte == 0));
        assert!(data[9..].iter().all(|byte| *byte == 0));
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
        assert!(range_is_ready(&instance, 8 * 1024, 4 * 1024));
    }

    #[tokio::test]
    async fn kuasar_partial_fault_fetches_complete_canonical_extent() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(16 * 1024).unwrap();
        let remote = mock_remote();
        let cfg = kuasar_config(
            file.path().to_path_buf(),
            16 * 1024,
            vec![DataExtent {
                offset: 4096,
                len: 8192,
            }],
        );
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote.clone(),
        ));

        instance.ensure_range(8192, 4096).await.unwrap();

        assert_eq!(remote.calls.lock().unwrap().as_slice(), &[(4096, 8192)]);
        assert!(range_is_ready(&instance, 4096, 8192));
    }

    #[tokio::test]
    async fn kuasar_zero_only_range_does_not_fetch_remote_data() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(16 * 1024).unwrap();
        let remote = mock_remote();
        let cfg = kuasar_config(
            file.path().to_path_buf(),
            16 * 1024,
            vec![DataExtent {
                offset: 8192,
                len: 4096,
            }],
        );
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote.clone(),
        ));

        instance.ensure_range(0, 4096).await.unwrap();

        assert_eq!(remote.reads.load(Ordering::SeqCst), 0);
        assert!(range_is_ready(&instance, 0, 4096));
    }

    #[tokio::test]
    async fn kuasar_page_fetches_all_intersecting_canonical_extents() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(4096).unwrap();
        let remote = mock_remote();
        let cfg = kuasar_config(
            file.path().to_path_buf(),
            4096,
            vec![
                DataExtent {
                    offset: 0,
                    len: 1024,
                },
                DataExtent {
                    offset: 2048,
                    len: 1024,
                },
            ],
        );
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote.clone(),
        ));

        let (range, _cache) = instance.prepare_fetch_range(0, 4096, 4096).await.unwrap();

        assert_eq!(range.off, 0);
        assert_eq!(range.len, 4096);
        assert_eq!(
            remote.calls.lock().unwrap().as_slice(),
            &[(0, 1024), (2048, 1024)]
        );
        assert!(range_is_ready(&instance, 0, 4096));
    }

    #[tokio::test]
    async fn kuasar_overlapping_faults_deduplicate_canonical_extent() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(8192).unwrap();
        let remote = Arc::new(MockRemote {
            reads: AtomicUsize::new(0),
            delay: Duration::from_millis(25),
            calls: Mutex::new(Vec::new()),
        });
        let cfg = kuasar_config(
            file.path().to_path_buf(),
            8192,
            vec![DataExtent {
                offset: 0,
                len: 8192,
            }],
        );
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote.clone(),
        ));

        let first = tokio::spawn({
            let instance = instance.clone();
            async move { instance.ensure_range(0, 4096).await }
        });
        let second = tokio::spawn({
            let instance = instance.clone();
            async move { instance.ensure_range(4096, 4096).await }
        });
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();

        assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
        assert_eq!(remote.calls.lock().unwrap().as_slice(), &[(0, 8192)]);
    }

    #[tokio::test]
    async fn kuasar_tail_page_keeps_bytes_after_blob_zero() {
        let blob_size = 4097;
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(8192).unwrap();
        let remote = Arc::new(StagingRemote {
            data: vec![b'k'; blob_size],
        });
        let cfg = kuasar_config(
            file.path().to_path_buf(),
            blob_size as u64,
            vec![DataExtent {
                offset: 4096,
                len: 1,
            }],
        );
        let instance = Arc::new(Instance::with_mock_remote(
            cfg,
            file.reopen().unwrap(),
            remote,
        ));

        instance
            .prepare_fetch_range(4096, 4096, 4096)
            .await
            .unwrap();

        let mut page = vec![0; 4096];
        file.as_file().read_exact_at(&mut page, 4096).unwrap();
        assert_eq!(page[0], b'k');
        assert!(page[1..].iter().all(|byte| *byte == 0));
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
        assert!(range_is_ready(&instance, 0, 1024 * 1024));
        assert!(range_is_ready(&instance, 1024 * 1024, 1024 * 1024));
        assert!(!range_is_ready(&instance, 2 * 1024 * 1024, 1));

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
        mark_fixed_range_ready(&instance, 16, 4);

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
