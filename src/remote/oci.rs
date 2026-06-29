use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use serde::Deserialize;

use reqwest::header::{ACCEPT, AUTHORIZATION, RANGE};
use reqwest::{Client, StatusCode, Url};

use crate::error::{Error, Result};
use crate::remote::{AuthConfig, BlobDescriptor, RemoteBackend, RemoteSource};

pub struct OciRemoteBackend {
    client: Client,
    blob_url: Url,
    auth: Option<AuthConfig>,
}

impl OciRemoteBackend {
    pub fn from_config(
        blob: &BlobDescriptor,
        source: &RemoteSource,
        auth: Option<AuthConfig>,
    ) -> Result<Self> {
        let RemoteSource::OciRegistry {
            image_ref,
            hosts_dir,
        } = source;
        let blob_url = build_blob_url(image_ref, hosts_dir.as_deref(), &blob.digest)?;
        Ok(Self {
            client: Client::new(),
            blob_url,
            auth,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageMetadata {
    pub config: BlobDescriptor,
    pub layers: Vec<BlobDescriptor>,
}

pub async fn resolve_image_metadata(
    image_ref: &str,
    hosts_dir: Option<&str>,
    auth: Option<AuthConfig>,
) -> Result<ImageMetadata> {
    let client = Client::new();
    let manifest = resolve_manifest(&client, image_ref, hosts_dir, auth.as_ref()).await?;
    let config = BlobDescriptor::from(manifest.config);
    let layers = manifest
        .layers
        .into_iter()
        .map(BlobDescriptor::from)
        .collect();
    fetch_config(&client, image_ref, hosts_dir, auth.as_ref(), &config).await?;
    Ok(ImageMetadata { config, layers })
}

async fn resolve_manifest(
    client: &Client,
    image_ref: &str,
    hosts_dir: Option<&str>,
    auth: Option<&AuthConfig>,
) -> Result<OciManifest> {
    let manifest_url = build_manifest_url(image_ref, hosts_dir, None)?;
    let value: serde_json::Value = get_json(client, manifest_url, auth, manifest_accept()).await?;
    let media_type = value
        .get("mediaType")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();

    if is_manifest_list(media_type) {
        let index: OciIndex = serde_json::from_value(value)?;
        let manifest = index
            .manifests
            .first()
            .ok_or_else(|| Error::Remote("image index contains no manifests".to_string()))?;
        let manifest_url = build_manifest_url(image_ref, hosts_dir, Some(&manifest.digest))?;
        return get_json(client, manifest_url, auth, manifest_accept()).await;
    }

    serde_json::from_value(value).map_err(Into::into)
}

async fn fetch_config(
    client: &Client,
    image_ref: &str,
    hosts_dir: Option<&str>,
    auth: Option<&AuthConfig>,
    config: &BlobDescriptor,
) -> Result<()> {
    let url = build_blob_url(image_ref, hosts_dir, &config.digest)?;
    let _: serde_json::Value = get_json(client, url, auth, "application/json").await?;
    Ok(())
}

async fn get_json<T>(
    client: &Client,
    url: Url,
    auth: Option<&AuthConfig>,
    accept: &'static str,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let mut request = client.get(url).header(ACCEPT, accept);
    if let Some(auth) = auth {
        request = request.header(AUTHORIZATION, basic_auth(auth));
    }
    let response = request.send().await?;
    if response.status() != StatusCode::OK {
        return Err(Error::Remote(format!(
            "registry metadata read failed with status {}",
            response.status()
        )));
    }
    response.json().await.map_err(Into::into)
}

#[async_trait::async_trait]
impl RemoteBackend for OciRemoteBackend {
    async fn read_range(&self, offset: u64, len: u64) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let end = offset
            .checked_add(len)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        let mut request = self
            .client
            .get(self.blob_url.clone())
            .header(RANGE, format!("bytes={offset}-{end}"));

        if let Some(auth) = &self.auth {
            request = request.header(AUTHORIZATION, basic_auth(auth));
        }

        let response = request.send().await?;
        if response.status() != StatusCode::PARTIAL_CONTENT && response.status() != StatusCode::OK {
            return Err(Error::Remote(format!(
                "registry range read failed with status {}",
                response.status()
            )));
        }
        let bytes = response.bytes().await?;
        if bytes.len() > len as usize {
            return Err(Error::Remote(
                "registry returned more bytes than requested".to_string(),
            ));
        }
        Ok(bytes)
    }
}

fn basic_auth(auth: &AuthConfig) -> String {
    let token = BASE64_STANDARD.encode(format!("{}:{}", auth.username, auth.secret));
    format!("Basic {token}")
}

fn build_manifest_url(
    image_ref: &str,
    hosts_dir: Option<&str>,
    digest: Option<&str>,
) -> Result<Url> {
    let parsed = parse_image_ref(image_ref)?;
    let reference = digest.unwrap_or(&parsed.reference);
    let base = hosts_dir
        .and_then(|dir| read_hosts_server(dir, &parsed.registry).transpose())
        .transpose()?
        .unwrap_or_else(|| parsed.default_base_url());
    let mut url = Url::parse(&base).map_err(|err| Error::BadRequest(err.to_string()))?;
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v2/");
    path.push_str(&parsed.repository);
    path.push_str("/manifests/");
    path.push_str(reference);
    url.set_path(&path);
    Ok(url)
}

fn build_blob_url(image_ref: &str, hosts_dir: Option<&str>, digest: &str) -> Result<Url> {
    let parsed = parse_image_ref(image_ref)?;
    let base = hosts_dir
        .and_then(|dir| read_hosts_server(dir, &parsed.registry).transpose())
        .transpose()?
        .unwrap_or_else(|| parsed.default_base_url());
    let mut url = Url::parse(&base).map_err(|err| Error::BadRequest(err.to_string()))?;
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v2/");
    path.push_str(&parsed.repository);
    path.push_str("/blobs/");
    path.push_str(digest);
    url.set_path(&path);
    Ok(url)
}

fn manifest_accept() -> &'static str {
    "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json"
}

fn is_manifest_list(media_type: &str) -> bool {
    matches!(
        media_type,
        "application/vnd.oci.image.index.v1+json"
            | "application/vnd.docker.distribution.manifest.list.v2+json"
    )
}

#[derive(Debug, Deserialize)]
struct OciIndex {
    manifests: Vec<OciDescriptor>,
}

#[derive(Debug, Deserialize)]
struct OciManifest {
    config: OciBlobDescriptor,
    layers: Vec<OciBlobDescriptor>,
}

#[derive(Debug, Deserialize)]
struct OciDescriptor {
    digest: String,
}

#[derive(Debug, Deserialize)]
struct OciBlobDescriptor {
    digest: String,
    size: u64,
    #[serde(default, rename = "mediaType")]
    media_type: Option<String>,
}

impl From<OciBlobDescriptor> for BlobDescriptor {
    fn from(value: OciBlobDescriptor) -> Self {
        Self {
            digest: value.digest,
            size: value.size,
            media_type: value.media_type,
        }
    }
}

fn read_hosts_server(hosts_dir: &str, registry: &str) -> Result<Option<String>> {
    let path = Path::new(hosts_dir).join(registry).join("hosts.toml");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)?;
    let value: toml::Value = content
        .parse()
        .map_err(|err: toml::de::Error| Error::BadRequest(err.to_string()))?;
    Ok(value
        .get("server")
        .and_then(toml::Value::as_str)
        .map(ToOwned::to_owned))
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedImageRef {
    scheme: String,
    registry: String,
    authority: String,
    repository: String,
    reference: String,
}

impl ParsedImageRef {
    fn default_base_url(&self) -> String {
        format!("{}://{}", self.scheme, self.authority)
    }
}

fn parse_image_ref(image_ref: &str) -> Result<ParsedImageRef> {
    let with_scheme = if image_ref.starts_with("http://") || image_ref.starts_with("https://") {
        image_ref.to_string()
    } else {
        format!("https://{image_ref}")
    };
    let url = Url::parse(&with_scheme).map_err(|err| Error::BadRequest(err.to_string()))?;
    let registry = url
        .host_str()
        .ok_or_else(|| Error::BadRequest("image_ref missing registry host".to_string()))?
        .to_string();
    let authority = match url.port() {
        Some(port) => format!("{registry}:{port}"),
        None => registry.clone(),
    };
    let scheme = url.scheme().to_string();
    let repository = url.path().trim_start_matches('/').to_string();
    if repository.is_empty() {
        return Err(Error::BadRequest(
            "image_ref missing repository".to_string(),
        ));
    }
    let (repository, reference) = if let Some((repo, digest)) = repository.rsplit_once('@') {
        (repo.to_string(), digest.to_string())
    } else if let Some((repo, tag)) = repository.rsplit_once(':') {
        (repo.to_string(), tag.to_string())
    } else {
        (repository, "latest".to_string())
    };
    Ok(ParsedImageRef {
        scheme,
        registry,
        authority,
        repository,
        reference,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::RemoteBackend;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn parses_image_ref() {
        assert_eq!(
            parse_image_ref("registry.example.com/ns/image:tag").unwrap(),
            ParsedImageRef {
                scheme: "https".to_string(),
                registry: "registry.example.com".to_string(),
                authority: "registry.example.com".to_string(),
                repository: "ns/image".to_string(),
                reference: "tag".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn resolves_manifest_layers_without_fetching_layer_blobs() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/ns/image/manifests/tag"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "sha256:config",
                    "size": 32
                },
                "layers": [{
                    "mediaType": "application/vnd.erofs.layer.v1",
                    "digest": "sha256:layer",
                    "size": 4096
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v2/ns/image/blobs/sha256:config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        let metadata =
            resolve_image_metadata(&format!("{}/ns/image:tag", server.uri()), None, None)
                .await
                .unwrap();

        assert_eq!(metadata.config.digest, "sha256:config");
        assert_eq!(
            metadata.layers,
            vec![BlobDescriptor {
                digest: "sha256:layer".to_string(),
                size: 4096,
                media_type: Some("application/vnd.erofs.layer.v1".to_string()),
            }]
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn reads_blob_range_with_basic_auth() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/ns/image/blobs/sha256:abc"))
            .and(header("range", "bytes=4-7"))
            .and(header("authorization", "Basic dXNlcjpwYXNz"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(b"data"))
            .mount(&server)
            .await;

        let blob = BlobDescriptor {
            digest: "sha256:abc".to_string(),
            size: 16,
            media_type: None,
        };
        let source = RemoteSource::OciRegistry {
            image_ref: format!("{}/ns/image:tag", server.uri()),
            hosts_dir: None,
        };
        let backend = OciRemoteBackend::from_config(
            &blob,
            &source,
            Some(AuthConfig {
                username: "user".to_string(),
                secret: "pass".to_string(),
            }),
        )
        .unwrap();

        assert_eq!(
            backend.read_range(4, 4).await.unwrap(),
            Bytes::from_static(b"data")
        );
    }

    #[tokio::test]
    async fn propagates_registry_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/ns/image/blobs/sha256:abc"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let blob = BlobDescriptor {
            digest: "sha256:abc".to_string(),
            size: 16,
            media_type: None,
        };
        let source = RemoteSource::OciRegistry {
            image_ref: format!("{}/ns/image:tag", server.uri()),
            hosts_dir: None,
        };
        let backend = OciRemoteBackend::from_config(&blob, &source, None).unwrap();

        assert!(matches!(
            backend.read_range(0, 4).await,
            Err(Error::Remote(_))
        ));
    }
}
