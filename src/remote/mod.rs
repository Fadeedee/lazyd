pub mod oci;

use std::fs::File;

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
    KuasarManifest {
        manifest_keys: Vec<String>,
        accelerator_socket: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthConfig {
    pub username: String,
    pub secret: String,
}

#[derive(Debug)]
pub enum RemoteRange {
    Bytes(Bytes),
    StagingFile { file: File, len: u64 },
}

impl RemoteRange {
    pub fn len(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::StagingFile { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_staging_file(self) -> Option<(File, u64)> {
        match self {
            Self::StagingFile { file, len } => Some((file, len)),
            Self::Bytes(_) => None,
        }
    }
}

#[async_trait]
pub trait RemoteBackend: Send + Sync {
    async fn read_range(&self, offset: u64, len: u64) -> Result<RemoteRange>;
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use super::*;

    #[test]
    fn kuasar_manifest_source_has_stable_json_schema() {
        let source = RemoteSource::KuasarManifest {
            manifest_keys: vec!["11".repeat(32), "22".repeat(32)],
            accelerator_socket: "/run/accelerator/manifest-range.sock".to_string(),
        };

        let encoded = serde_json::to_value(&source).unwrap();
        assert_eq!(encoded["type"], "kuasar-manifest");
        assert_eq!(encoded["manifest_keys"][0], "11".repeat(32));
        assert_eq!(
            encoded["accelerator_socket"],
            "/run/accelerator/manifest-range.sock"
        );
        assert_eq!(
            serde_json::from_value::<RemoteSource>(encoded).unwrap(),
            source
        );
    }

    #[test]
    fn remote_range_reports_memory_and_staging_file_lengths() {
        let bytes = RemoteRange::Bytes(Bytes::from_static(b"data"));
        let file = RemoteRange::StagingFile {
            file: tempfile().unwrap(),
            len: 8192,
        };

        assert_eq!(bytes.len(), 4);
        assert!(!bytes.is_empty());
        assert_eq!(file.len(), 8192);
        assert!(!file.is_empty());
        assert_eq!(file.into_staging_file().unwrap().1, 8192);
    }
}
