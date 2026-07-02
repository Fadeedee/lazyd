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

pub fn prepare_cache_layers(
    cache_root: &Path,
    request: &PrepareImageRequest,
    layers: &[PrepareLayerDescriptor],
) -> Result<Vec<PreparedLayer>> {
    validate_fetch_unit_bytes(request.fetch.unit_bytes)?;
    let mut prepared = Vec::with_capacity(layers.len());
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
        let cache_key = cache_key(&layer.digest);
        let layer_dir = cache_root.join(&cache_key);
        std::fs::create_dir_all(&layer_dir)?;
        let sparse_path = layer_dir.join("layer.erofs");
        create_sparse_file(&sparse_path, pmem_size)?;
        let target = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sparse_path)?;
        let blob = layer.blob();
        let opened = RangeMap::open_or_create(&sparse_path, &blob, request.fetch.unit_bytes)?;
        if opened.needs_recovery {
            opened.range_map.recovery_reconcile(&target)?;
        }

        prepared.push(PreparedLayer {
            index: layer.index,
            sparse_path: sparse_path.clone(),
            bitmap_path: bitmap_path(&sparse_path),
            blob_digest: layer.digest.clone(),
            blob_size: layer.size,
            pmem_size,
            media_type: layer.media_type.clone(),
            instance_id: instance_id(&cache_key),
        });
    }
    Ok(prepared)
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

fn cache_key(digest: &str) -> String {
    digest
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn instance_id(cache_key: &str) -> String {
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
    use tempfile::tempdir;

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
                digest: "sha256:layer".to_string(),
                size: 3 * 1024 * 1024 + 1,
                media_type: EROFS_LAYER_MEDIA_TYPE.to_string(),
            }],
        )
        .unwrap();

        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].index, 7);
        assert_eq!(prepared[0].pmem_size, 4 * 1024 * 1024);
        assert_eq!(prepared[0].instance_id, "erofs-sha256-layer");
        assert_eq!(
            std::fs::metadata(&prepared[0].sparse_path).unwrap().len(),
            4 * 1024 * 1024
        );
        assert!(prepared[0].bitmap_path.exists());
    }

    #[test]
    fn rejects_non_erofs_layer() {
        let dir = tempdir().unwrap();
        let err = prepare_cache_layers(
            dir.path(),
            &request(1024 * 1024, 2 * 1024 * 1024),
            &[PrepareLayerDescriptor {
                index: 0,
                digest: "sha256:layer".to_string(),
                size: 4096,
                media_type: "application/vnd.oci.image.layer.v1.tar".to_string(),
            }],
        )
        .unwrap_err();

        assert!(matches!(err, Error::BadRequest(_)));
    }
}
