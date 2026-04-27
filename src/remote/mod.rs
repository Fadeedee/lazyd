pub mod oci;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobDescriptor {
    pub digest: String,
    pub size: u64,
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum RemoteSource {
    OciRegistry {
        image_ref: String,
        #[serde(default)]
        hosts_dir: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthConfig {
    pub username: String,
    pub secret: String,
}

#[async_trait]
pub trait RemoteBackend: Send + Sync {
    async fn read_range(&self, offset: u64, len: u64) -> Result<Bytes>;
}
