use serde::Deserialize;

use crate::instance::DEFAULT_FETCH_UNIT_BYTES;
use crate::remote::AuthConfig;

pub const DEFAULT_PMEM_ALIGNMENT_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct PrepareImageRequest {
    pub image_ref: String,
    #[serde(default)]
    pub hosts_dir: Option<String>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub fetch: PrepareFetchConfig,
    #[serde(default)]
    pub pmem: PreparePmemConfig,
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

fn default_fetch_unit_bytes() -> u64 {
    DEFAULT_FETCH_UNIT_BYTES
}

fn default_pmem_alignment_bytes() -> u64 {
    DEFAULT_PMEM_ALIGNMENT_BYTES
}
