use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::instance::DEFAULT_FETCH_UNIT_BYTES;
use crate::range_map::{RangeMap, bitmap_path, validate_fetch_unit_bytes};
use crate::remote::{AuthConfig, BlobDescriptor};
use crate::{error::Error, error::Result};

pub const EROFS_LAYER_MEDIA_TYPE: &str = "application/vnd.erofs.layer.v1";
pub const DEFAULT_PMEM_ALIGNMENT_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct PrepareImageRequest {
    pub image_ref: String,
    #[serde(default)]
    pub hosts_dir: Option<String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub layers: Vec<PrepareLayerDescriptor>,
    #[serde(default)]
    pub fetch: PrepareFetchConfig,
    #[serde(default)]
    pub pmem: PreparePmemConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PrepareLayerDescriptor {
    pub index: u32,
    pub digest: String,
    pub size: u64,
    pub media_type: String,
}

impl PrepareLayerDescriptor {
    pub fn blob(&self) -> BlobDescriptor {
        BlobDescriptor {
            digest: self.digest.clone(),
            size: self.size,
            media_type: Some(self.media_type.clone()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PrepareFetchConfig {
    #[serde(default = "default_fetch_unit_bytes")]
    pub unit_bytes: u64,
}

impl Default for PrepareFetchConfig {
    fn default() -> Self {
        Self {
            unit_bytes: DEFAULT_FETCH_UNIT_BYTES,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PreparePmemConfig {
    #[serde(default = "default_pmem_alignment_bytes")]
    pub alignment_bytes: u64,
}

impl Default for PreparePmemConfig {
    fn default() -> Self {
        Self {
            alignment_bytes: DEFAULT_PMEM_ALIGNMENT_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreparedLayer {
    pub index: u32,
    pub sparse_path: PathBuf,
    pub bitmap_path: PathBuf,
    pub blob_digest: String,
    pub blob_size: u64,
    pub pmem_size: u64,
    pub media_type: String,
    pub instance_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrepareImageResponse {
    pub layers: Vec<PreparedLayer>,
}

struct LayerPlan {
    layer: PrepareLayerDescriptor,
    sparse_path: PathBuf,
    cache_key: String,
    pmem_size: u64,
}

pub fn prepare_cache_layers(
    cache_root: &Path,
    request: &PrepareImageRequest,
    layers: &[PrepareLayerDescriptor],
) -> Result<Vec<PreparedLayer>> {
    validate_fetch_unit_bytes(request.fetch.unit_bytes)?;
    let mut plans = Vec::with_capacity(layers.len());
    for layer in layers {
        if layer.media_type != EROFS_LAYER_MEDIA_TYPE {
            return Err(Error::BadRequest(format!(
                "layer {} media type {} is not native EROFS; convert rootfs to native EROFS and push first",
                layer.index, layer.media_type
            )));
        }
        if layer.size == 0 {
            return Err(Error::BadRequest(format!(
                "layer {} blob size must be greater than zero",
                layer.index
            )));
        }

        let pmem_size = align_up(layer.size, request.pmem.alignment_bytes)?;
        let cache_key = cache_key(&layer.digest)?;
        let layer_dir = cache_root.join(&cache_key);
        let sparse_path = layer_dir.join("layer.erofs");
        validate_existing_cache(
            &sparse_path,
            pmem_size,
            &layer.blob(),
            request.fetch.unit_bytes,
        )?;
        plans.push(LayerPlan {
            layer: layer.clone(),
            sparse_path,
            cache_key,
            pmem_size,
        });
    }

    let mut prepared = Vec::with_capacity(plans.len());
    for plan in plans {
        let layer_dir = plan.sparse_path.parent().ok_or_else(|| {
            Error::BadRequest("sparse cache path has no parent directory".to_string())
        })?;
        std::fs::create_dir_all(layer_dir)?;
        create_sparse_file(&plan.sparse_path, plan.pmem_size)?;
        let target = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&plan.sparse_path)?;
        let blob = plan.layer.blob();
        let opened = RangeMap::open_or_create(&plan.sparse_path, &blob, request.fetch.unit_bytes)?;
        if opened.needs_recovery {
            opened.range_map.recovery_reconcile(&target)?;
        }

        prepared.push(PreparedLayer {
            index: plan.layer.index,
            sparse_path: plan.sparse_path.clone(),
            bitmap_path: bitmap_path(&plan.sparse_path),
            blob_digest: plan.layer.digest.clone(),
            blob_size: plan.layer.size,
            pmem_size: plan.pmem_size,
            media_type: plan.layer.media_type.clone(),
            instance_id: instance_id(&plan.cache_key),
        });
    }
    Ok(prepared)
}

fn validate_existing_cache(
    path: &Path,
    expected_len: u64,
    blob: &BlobDescriptor,
    unit_bytes: u64,
) -> Result<()> {
    if path.exists() {
        let actual_len = std::fs::metadata(path)?.len();
        if actual_len != expected_len {
            return Err(Error::Conflict(format!(
                "existing sparse cache {} has size {}, expected {}",
                path.display(),
                actual_len,
                expected_len
            )));
        }
    }
    RangeMap::validate_existing(path, blob, unit_bytes)
}

fn create_sparse_file(path: &Path, len: u64) -> Result<()> {
    let existed = path.exists();
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    let current = file.metadata()?.len();
    if !existed || current < len {
        file.set_len(len)?;
    } else if current > len {
        return Err(Error::Conflict(format!(
            "existing sparse cache {} is larger than expected",
            path.display()
        )));
    }
    Ok(())
}

fn align_up(value: u64, alignment: u64) -> Result<u64> {
    if alignment == 0 {
        return Err(Error::BadRequest(
            "pmem.alignment_bytes must be greater than zero".to_string(),
        ));
    }
    value
        .checked_add(alignment - 1)
        .map(|value| (value / alignment) * alignment)
        .ok_or_else(|| Error::BadRequest("pmem size overflows u64".to_string()))
}

pub(crate) fn cache_key(digest: &str) -> Result<String> {
    let encoded = digest.strip_prefix("sha256:").ok_or_else(|| {
        Error::BadRequest("layer digest must be canonical sha256:<64 lowercase hex>".to_string())
    })?;
    if encoded.len() != 64
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::BadRequest(
            "layer digest must be canonical sha256:<64 lowercase hex>".to_string(),
        ));
    }
    Ok(format!("sha256-{encoded}"))
}

pub(crate) fn instance_id(cache_key: &str) -> String {
    format!("erofs-{cache_key}")
}

fn default_fetch_unit_bytes() -> u64 {
    DEFAULT_FETCH_UNIT_BYTES
}

fn default_pmem_alignment_bytes() -> u64 {
    DEFAULT_PMEM_ALIGNMENT_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range_map::BITMAP_UNIT_BYTES;
    use tempfile::tempdir;

    const TEST_DIGEST: &str =
        "sha256:1111111111111111111111111111111111111111111111111111111111111111";

    fn request(unit_bytes: u64, alignment_bytes: u64) -> PrepareImageRequest {
        PrepareImageRequest {
            image_ref: "registry.example.com/ns/image:tag".to_string(),
            hosts_dir: None,
            auth: None,
            layers: Vec::new(),
            fetch: PrepareFetchConfig { unit_bytes },
            pmem: PreparePmemConfig { alignment_bytes },
        }
    }

    #[test]
    fn creates_sparse_cache_and_bitmap_for_erofs_layer() {
        let dir = tempdir().unwrap();
        let prepared = prepare_cache_layers(
            dir.path(),
            &request(1024 * 1024, 2 * 1024 * 1024),
            &[PrepareLayerDescriptor {
                index: 7,
                digest: TEST_DIGEST.to_string(),
                size: 3 * 1024 * 1024 + 1,
                media_type: EROFS_LAYER_MEDIA_TYPE.to_string(),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].index, 7);
        assert_eq!(prepared[0].pmem_size, 4 * 1024 * 1024);
        assert_eq!(
            prepared[0].instance_id,
            "erofs-sha256-1111111111111111111111111111111111111111111111111111111111111111"
        );
        assert_eq!(
            std::fs::metadata(&prepared[0].sparse_path).unwrap().len(),
            4 * 1024 * 1024
        );
        assert!(prepared[0].bitmap_path.exists());
    }

    #[test]
    fn conflicting_prepare_does_not_modify_existing_cache_or_bitmap() {
        let dir = tempdir().unwrap();
        let first = prepare_cache_layers(
            dir.path(),
            &request(BITMAP_UNIT_BYTES, 2 * 1024 * 1024),
            &[PrepareLayerDescriptor {
                index: 0,
                digest: TEST_DIGEST.to_string(),
                size: BITMAP_UNIT_BYTES,
                media_type: EROFS_LAYER_MEDIA_TYPE.to_string(),
            }],
        )
        .unwrap();
        let sparse_len = std::fs::metadata(&first[0].sparse_path).unwrap().len();
        let bitmap = std::fs::read(&first[0].bitmap_path).unwrap();

        let err = prepare_cache_layers(
            dir.path(),
            &request(BITMAP_UNIT_BYTES, 2 * 1024 * 1024),
            &[PrepareLayerDescriptor {
                index: 7,
                digest: TEST_DIGEST.to_string(),
                size: 3 * BITMAP_UNIT_BYTES,
                media_type: EROFS_LAYER_MEDIA_TYPE.to_string(),
            }],
        )
        .unwrap_err();

        assert!(matches!(err, Error::Conflict(_)));
        assert_eq!(
            std::fs::metadata(&first[0].sparse_path).unwrap().len(),
            sparse_len
        );
        assert_eq!(std::fs::read(&first[0].bitmap_path).unwrap(), bitmap);

        let err = prepare_cache_layers(
            dir.path(),
            &request(2 * BITMAP_UNIT_BYTES, 2 * 1024 * 1024),
            &[PrepareLayerDescriptor {
                index: 8,
                digest: TEST_DIGEST.to_string(),
                size: BITMAP_UNIT_BYTES,
                media_type: EROFS_LAYER_MEDIA_TYPE.to_string(),
            }],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Conflict(_)));
        assert_eq!(
            std::fs::metadata(&first[0].sparse_path).unwrap().len(),
            sparse_len
        );
        assert_eq!(std::fs::read(&first[0].bitmap_path).unwrap(), bitmap);
    }

    #[test]
    fn rejects_non_erofs_layer() {
        let dir = tempdir().unwrap();
        let err = prepare_cache_layers(
            dir.path(),
            &request(1024 * 1024, 2 * 1024 * 1024),
            &[PrepareLayerDescriptor {
                index: 0,
                digest: TEST_DIGEST.to_string(),
                size: 4096,
                media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
            }],
        )
        .unwrap_err();

        assert!(matches!(err, Error::BadRequest(_)));
    }

    #[test]
    fn rejects_non_canonical_sha256_digest() {
        let dir = tempdir().unwrap();
        let err = prepare_cache_layers(
            dir.path(),
            &request(1024 * 1024, 2 * 1024 * 1024),
            &[PrepareLayerDescriptor {
                index: 0,
                digest: "sha256:layer/path".to_string(),
                size: 4096,
                media_type: EROFS_LAYER_MEDIA_TYPE.to_string(),
            }],
        )
        .unwrap_err();

        assert!(matches!(err, Error::BadRequest(msg) if msg.contains("canonical sha256")));
    }
}
