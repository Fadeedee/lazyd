use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::fanotify::FanotifyBackend;
use crate::remote::oci::OciRemoteBackend;
use crate::remote::{AuthConfig, BlobDescriptor, RemoteBackend, RemoteSource};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceConfig {
    #[serde(skip)]
    pub instance_id: String,
    pub target_path: PathBuf,
    pub blob: BlobDescriptor,
    pub source: RemoteSource,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
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
        let target = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&config.target_path)?;
        let remote = Arc::new(OciRemoteBackend::from_config(
            &config.blob,
            &config.source,
            config.auth.clone(),
        )?) as Arc<dyn RemoteBackend>;
        Ok(Self::with_remote(config, target, remote))
    }

    #[cfg(test)]
    fn with_mock_remote(
        mut config: InstanceConfig,
        target: std::fs::File,
        remote: Arc<dyn RemoteBackend>,
    ) -> Self {
        config.instance_id = "test".to_string();
        Self::with_remote(config, target, remote)
    }

    fn with_remote(
        config: InstanceConfig,
        target: std::fs::File,
        remote: Arc<dyn RemoteBackend>,
    ) -> Self {
        Self {
            config,
            target,
            remote,
            inflight: Mutex::new(Vec::new()),
        }
    }

    pub async fn ensure_range(&self, offset: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        if end > self.config.blob.size {
            return Err(Error::BadRequest("range exceeds blob size".to_string()));
        }
        if self.range_is_present(offset, len)? {
            return Ok(());
        }
        let range = Range { offset, len };
        let _guard = self.reserve_inflight(range).await?;
        if self.range_is_present(offset, len)? {
            return Ok(());
        }
        let bytes = self.remote.read_range(offset, len).await?;
        if bytes.len() != len as usize {
            return Err(Error::Remote(format!(
                "remote returned {} bytes, expected {}",
                bytes.len(),
                len
            )));
        }
        self.write_all_at(&bytes, offset)?;
        Ok(())
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

    fn range_is_present(&self, offset: u64, len: u64) -> Result<bool> {
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        let data = unsafe {
            libc::lseek(
                self.target.as_raw_fd(),
                offset as libc::off_t,
                libc::SEEK_DATA,
            )
        };
        if data < 0 {
            let err = std::io::Error::last_os_error();
            return match err.raw_os_error() {
                Some(libc::ENXIO) => Ok(false),
                Some(libc::EINVAL) => Ok(false),
                _ => Err(err.into()),
            };
        }
        if data as u64 > offset {
            return Ok(false);
        }
        let hole = unsafe {
            libc::lseek(
                self.target.as_raw_fd(),
                offset as libc::off_t,
                libc::SEEK_HOLE,
            )
        };
        if hole < 0 {
            let err = std::io::Error::last_os_error();
            return match err.raw_os_error() {
                Some(libc::ENXIO) | Some(libc::EINVAL) => Ok(false),
                _ => Err(err.into()),
            };
        }
        Ok(hole as u64 >= end)
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

use std::os::fd::AsRawFd;

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
    }

    #[async_trait]
    impl RemoteBackend for MockRemote {
        async fn read_range(&self, offset: u64, len: u64) -> Result<Bytes> {
            self.reads.fetch_add(1, Ordering::SeqCst);
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
        let instance = Instance::with_mock_remote(
            config(file.path().to_path_buf()),
            file.reopen().unwrap(),
            remote.clone(),
        );

        instance.ensure_range(8, 4).await.unwrap();

        let mut buf = [0; 4];
        file.as_file().read_at(&mut buf, 8).unwrap();
        assert_eq!(buf, [8, 8, 8, 8]);
        assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn present_range_does_not_read_remote() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        file.as_file().write_all_at(b"xxxx", 16).unwrap();
        let remote = mock_remote();
        let instance = Instance::with_mock_remote(
            config(file.path().to_path_buf()),
            file.reopen().unwrap(),
            remote.clone(),
        );

        instance.ensure_range(16, 4).await.unwrap();

        assert_eq!(remote.reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn range_must_not_exceed_blob_size() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        let remote = mock_remote();
        let instance = Instance::with_mock_remote(
            config(file.path().to_path_buf()),
            file.reopen().unwrap(),
            remote,
        );

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
