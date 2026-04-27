use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, RANGE};
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
            let token = BASE64_STANDARD.encode(format!("{}:{}", auth.username, auth.secret));
            request = request.header(AUTHORIZATION, format!("Basic {token}"));
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
    let mut repository = url.path().trim_start_matches('/').to_string();
    if repository.is_empty() {
        return Err(Error::BadRequest(
            "image_ref missing repository".to_string(),
        ));
    }
    if let Some((repo, _tag)) = repository.rsplit_once(':') {
        repository = repo.to_string();
    }
    if let Some((repo, _digest)) = repository.rsplit_once('@') {
        repository = repo.to_string();
    }
    Ok(ParsedImageRef {
        scheme,
        registry,
        authority,
        repository,
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
            }
        );
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
