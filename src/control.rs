use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::instance::{FetchConfig, InstanceConfig, InstanceRegistry, TriggerMode};
use crate::prepare::{
    EROFS_IMAGE_MEDIA_TYPE, EROFS_LAYER_MEDIA_TYPE, PrepareImageRequest, PrepareImageResponse,
    PrepareLayerDescriptor, prepare_cache_layers,
};
use crate::remote::RemoteSource;
use crate::remote::kuasar::AcceleratorClient;

const DEFAULT_IMAGE_CACHE_DIR: &str = "/var/lib/lazyd/images";

#[derive(Clone)]
pub struct ControlPlane {
    registry: InstanceRegistry,
    image_cache_dir: PathBuf,
    prepare_lock: Arc<Mutex<()>>,
}

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
}

#[derive(Serialize)]
struct DaemonInfo {
    state: &'static str,
    instances: usize,
    io_backends: [&'static str; 2],
    remote_backend: &'static str,
}

#[derive(Debug, Deserialize)]
struct EnsureRangeRequest {
    offset: u64,
    len: u64,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl ControlPlane {
    pub fn new(registry: InstanceRegistry) -> Self {
        Self::with_image_cache_dir(registry, PathBuf::from(DEFAULT_IMAGE_CACHE_DIR))
    }

    pub fn with_image_cache_dir(registry: InstanceRegistry, image_cache_dir: PathBuf) -> Self {
        Self {
            registry,
            image_cache_dir,
            prepare_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn serve(&self, socket: PathBuf) -> Result<()> {
        if socket.exists() {
            tokio::fs::remove_file(&socket).await?;
        }
        if let Some(parent) = socket.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let listener = UnixListener::bind(socket)?;
        loop {
            let (stream, _) = listener.accept().await?;
            let control = self.clone();
            tokio::spawn(async move {
                if let Err(err) = control.handle_stream(stream).await {
                    tracing::warn!(%err, "control request failed");
                }
            });
        }
    }

    async fn handle_stream(&self, mut stream: UnixStream) -> Result<()> {
        let request = read_request(&mut stream).await?;
        let response = self.handle_request(request).await;
        write_response(&mut stream, response).await
    }

    async fn handle_request(&self, request: Request) -> Result<Response> {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/api/v1/daemon") => {
                let body = serde_json::to_vec(&DaemonInfo {
                    state: "running",
                    instances: self.registry.len().await,
                    io_backends: ["fanotify", "external"],
                    remote_backend: "oci-registry",
                })?;
                Ok(Response::json(200, body))
            }
            ("POST", "/api/v1/images/prepare") => {
                let prepare: PrepareImageRequest = serde_json::from_slice(&request.body)?;
                validate_prepare_request(&prepare)?;
                let layers = self.prepare_image(prepare).await?;
                let body = serde_json::to_vec(&PrepareImageResponse { layers })?;
                Ok(Response::json(200, body))
            }
            _ if request.method == "POST" => {
                let instance_id = ensure_range_instance_id(&request.path)
                    .ok_or_else(|| Error::NotFound("unknown endpoint".to_string()))?;
                let range: EnsureRangeRequest = serde_json::from_slice(&request.body)?;
                let instance = self
                    .registry
                    .get(instance_id)
                    .await
                    .ok_or_else(|| Error::NotFound("instance not found".to_string()))?;
                instance.ensure_range(range.offset, range.len).await?;
                Ok(Response::empty(204))
            }
            _ if request.method == "PUT" => {
                let instance_id = instance_id(&request.path)
                    .ok_or_else(|| Error::NotFound("unknown endpoint".to_string()))?
                    .to_string();
                let mut config: InstanceConfig = serde_json::from_slice(&request.body)?;
                config.instance_id = instance_id.clone();
                self.registry.register(instance_id, config).await?;
                Ok(Response::empty(204))
            }
            _ if request.method == "DELETE" => {
                let instance_id = instance_id(&request.path)
                    .ok_or_else(|| Error::NotFound("unknown endpoint".to_string()))?;
                self.registry.unregister(instance_id).await?;
                Ok(Response::empty(204))
            }
            _ => Err(Error::NotFound("unknown endpoint".to_string())),
        }
    }

    async fn prepare_image(
        &self,
        request: PrepareImageRequest,
    ) -> Result<Vec<crate::prepare::PreparedLayer>> {
        let (layers, source) = resolve_prepare_source(&request).await?;
        let _guard = self.prepare_lock.lock().await;
        let prepared = prepare_cache_layers(&self.image_cache_dir, &request, &layers)?;
        for (layer, prepared_layer) in layers.iter().zip(prepared.iter()) {
            self.registry
                .register(
                    prepared_layer.instance_id.clone(),
                    InstanceConfig {
                        instance_id: String::new(),
                        target_path: prepared_layer.sparse_path.clone(),
                        blob: layer.blob(),
                        source: source.clone(),
                        auth: matches!(&source, RemoteSource::OciRegistry { .. })
                            .then(|| request.auth.clone())
                            .flatten(),
                        fetch: FetchConfig {
                            unit_bytes: request.fetch.unit_bytes,
                        },
                        trigger_mode: TriggerMode::External,
                    },
                )
                .await?;
        }
        Ok(prepared)
    }
}

async fn resolve_prepare_source(
    request: &PrepareImageRequest,
) -> Result<(Vec<PrepareLayerDescriptor>, RemoteSource)> {
    match &request.source {
        None => Ok((
            request.layers.clone(),
            RemoteSource::OciRegistry {
                image_ref: request.image_ref.clone(),
                hosts_dir: request.hosts_dir.clone(),
            },
        )),
        Some(
            source @ RemoteSource::KuasarManifest {
                manifest_keys,
                accelerator_socket,
            },
        ) => {
            let image = AcceleratorClient::new(PathBuf::from(accelerator_socket))?
                .describe(manifest_keys)
                .await?;
            Ok((
                vec![PrepareLayerDescriptor {
                    index: 0,
                    digest: image.content_id,
                    size: image.image_size,
                    media_type: EROFS_IMAGE_MEDIA_TYPE.to_string(),
                }],
                source.clone(),
            ))
        }
        Some(RemoteSource::OciRegistry { .. }) => Err(Error::BadRequest(
            "OCI prepare uses top-level image_ref/hosts_dir and explicit layers".to_string(),
        )),
    }
}

fn validate_prepare_request(request: &PrepareImageRequest) -> Result<()> {
    if request.image_ref.trim().is_empty() {
        return Err(Error::BadRequest("image_ref is required".to_string()));
    }
    match &request.source {
        None => {
            if request.layers.is_empty() {
                return Err(Error::BadRequest("layers is required".to_string()));
            }
            if request
                .layers
                .iter()
                .any(|layer| layer.media_type != EROFS_LAYER_MEDIA_TYPE)
            {
                return Err(Error::BadRequest(
                    "OCI rootfs descriptors must use native EROFS layer media type; convert rootfs to native EROFS and push first"
                        .to_string(),
                ));
            }
        }
        Some(RemoteSource::KuasarManifest { .. }) => {
            if !request.layers.is_empty() {
                return Err(Error::BadRequest(
                    "layers must be omitted for a kuasar-manifest source".to_string(),
                ));
            }
            if !request.image_ref.starts_with("manifest://") {
                return Err(Error::BadRequest(
                    "kuasar-manifest image_ref must start with manifest://".to_string(),
                ));
            }
        }
        Some(RemoteSource::OciRegistry { .. }) => {
            return Err(Error::BadRequest(
                "OCI prepare uses top-level image_ref/hosts_dir and explicit layers".to_string(),
            ));
        }
    }
    if request.fetch.unit_bytes == 0 {
        return Err(Error::BadRequest(
            "fetch.unit_bytes must be greater than zero".to_string(),
        ));
    }
    if request.pmem.alignment_bytes == 0 {
        return Err(Error::BadRequest(
            "pmem.alignment_bytes must be greater than zero".to_string(),
        ));
    }
    let _ = (&request.hosts_dir, &request.auth);
    Ok(())
}

fn instance_id(path: &str) -> Option<&str> {
    let instance_id = path.strip_prefix("/api/v1/instances/")?;
    (!instance_id.is_empty() && !instance_id.contains('/')).then_some(instance_id)
}

fn ensure_range_instance_id(path: &str) -> Option<&str> {
    let instance_id = path
        .strip_prefix("/api/v1/instances/")?
        .strip_suffix("/ranges/ensure")?;
    (!instance_id.is_empty() && !instance_id.contains('/')).then_some(instance_id)
}

async fn read_request(stream: &mut UnixStream) -> Result<Request> {
    let mut buf = Vec::new();
    let mut tmp = [0; 4096];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some((headers_end, content_len)) = parse_headers(&buf)? {
            let total = headers_end + content_len;
            while buf.len() < total {
                let n = stream.read(&mut tmp).await?;
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            break;
        }
        if buf.len() > 1024 * 1024 {
            return Err(Error::BadRequest("request too large".to_string()));
        }
    }

    let header_end = find_header_end(&buf)
        .ok_or_else(|| Error::BadRequest("malformed HTTP request".to_string()))?;
    let head = std::str::from_utf8(&buf[..header_end - 4])
        .map_err(|err| Error::BadRequest(err.to_string()))?;
    let mut lines = head.lines();
    let line = lines
        .next()
        .ok_or_else(|| Error::BadRequest("missing request line".to_string()))?;
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| Error::BadRequest("missing method".to_string()))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| Error::BadRequest("missing path".to_string()))?
        .to_string();
    Ok(Request {
        method,
        path,
        body: buf[header_end..].to_vec(),
    })
}

fn parse_headers(buf: &[u8]) -> Result<Option<(usize, usize)>> {
    let Some(header_end) = find_header_end(buf) else {
        return Ok(None);
    };
    let head = std::str::from_utf8(&buf[..header_end - 4])
        .map_err(|err| Error::BadRequest(err.to_string()))?;
    let content_len = head
        .lines()
        .skip(1)
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    Ok(Some((header_end, content_len)))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

#[derive(Debug)]
struct Response {
    status: u16,
    content_type: Option<&'static str>,
    body: Vec<u8>,
}

impl Response {
    fn empty(status: u16) -> Self {
        Self {
            status,
            content_type: None,
            body: Vec::new(),
        }
    }

    fn json(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: Some("application/json"),
            body,
        }
    }
}

fn render_response(response: Result<Response>) -> Result<Vec<u8>> {
    let response = match response {
        Ok(response) => response,
        Err(err) => Response::json(
            err.status_code(),
            serde_json::to_vec(&ErrorBody {
                error: err.message(),
            })?,
        ),
    };
    let reason = match response.status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Internal Server Error",
    };
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        reason,
        response.body.len()
    );
    if let Some(content_type) = response.content_type {
        head.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    head.push_str("\r\n");
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(&response.body);
    Ok(bytes)
}

async fn write_response(stream: &mut UnixStream, response: Result<Response>) -> Result<()> {
    let bytes = render_response(response)?;
    stream.write_all(&bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs::File;
    use std::io::{Seek, SeekFrom, Write};
    use std::os::fd::{AsRawFd, FromRawFd};

    use super::*;
    use crate::data::SeqpacketListener;
    use crate::instance::InstanceRegistry;
    use crate::prepare::EROFS_LAYER_MEDIA_TYPE;
    use crate::remote::{BlobDescriptor, RemoteSource};
    use tempfile::{NamedTempFile, tempdir};
    use wiremock::MockServer;

    #[tokio::test]
    async fn daemon_info_returns_backend_state() {
        let control = ControlPlane::new(InstanceRegistry::new(None));
        let response = control
            .handle_request(Request {
                method: "GET".to_string(),
                path: "/api/v1/daemon".to_string(),
                body: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(value["state"], "running");
        assert_eq!(value["instances"], 0);
        assert_eq!(
            value["io_backends"],
            serde_json::json!(["fanotify", "external"])
        );
        assert_eq!(value["remote_backend"], "oci-registry");
    }

    #[tokio::test]
    async fn external_instance_can_ensure_a_zero_length_range() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(128).unwrap();
        let registry = InstanceRegistry::new(None);
        registry
            .register(
                "external".to_string(),
                InstanceConfig {
                    instance_id: String::new(),
                    target_path: file.path().to_path_buf(),
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
                    trigger_mode: TriggerMode::External,
                },
            )
            .await
            .unwrap();
        let control = ControlPlane::new(registry);

        let response = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/instances/external/ranges/ensure".to_string(),
                body: br#"{"offset":0,"len":0}"#.to_vec(),
            })
            .await
            .unwrap();

        assert_eq!(response.status, 204);
    }

    #[tokio::test]
    async fn prepare_image_creates_cache_and_registers_instances() {
        let server = MockServer::start().await;
        let cache = tempdir().unwrap();
        let registry = InstanceRegistry::new(None);
        let control = ControlPlane::with_image_cache_dir(registry.clone(), cache.path().into());

        let response = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body: format!(
                    r#"{{"image_ref":"{}/ns/image:tag","layers":[{{"index":0,"digest":"sha256:layer","size":4097,"media_type":"{}"}}],"fetch":{{"unit_bytes":1048576}},"pmem":{{"alignment_bytes":2097152}}}}"#,
                    server.uri(),
                    EROFS_LAYER_MEDIA_TYPE
                )
                .into_bytes(),
            })
            .await
            .unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(registry.len().await, 1);
        assert!(server.received_requests().await.unwrap().is_empty());
        let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(value["layers"][0]["index"], 0);
        assert_eq!(value["layers"][0]["blob_digest"], "sha256:layer");
        assert_eq!(value["layers"][0]["blob_size"], 4097);
        assert_eq!(value["layers"][0]["pmem_size"], 2 * 1024 * 1024);
        assert_eq!(value["layers"][0]["instance_id"], "erofs-sha256-layer");
        assert!(PathBuf::from(value["layers"][0]["sparse_path"].as_str().unwrap()).exists());
        assert!(PathBuf::from(value["layers"][0]["bitmap_path"].as_str().unwrap()).exists());
    }

    #[tokio::test]
    async fn prepare_image_requires_explicit_layers() {
        let control = ControlPlane::new(InstanceRegistry::new(None));

        let err = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body: br#"{"image_ref":"registry.example.com/ns/image:tag"}"#.to_vec(),
            })
            .await
            .unwrap_err();

        assert!(matches!(err, Error::BadRequest(msg) if msg == "layers is required"));
    }

    #[tokio::test]
    async fn prepare_image_rejects_non_erofs_descriptor() {
        let server = MockServer::start().await;
        let cache = tempdir().unwrap();
        let registry = InstanceRegistry::new(None);
        let control = ControlPlane::with_image_cache_dir(registry.clone(), cache.path().into());

        let err = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body: format!(
                    r#"{{"image_ref":"{}/ns/image:tag","layers":[{{"index":0,"digest":"sha256:layer","size":4097,"media_type":"application/vnd.oci.image.layer.v1.tar"}}]}}"#,
                    server.uri()
                )
                .into_bytes(),
            })
            .await
            .unwrap_err();

        assert_eq!(registry.len().await, 0);
        assert!(matches!(err, Error::BadRequest(_)));
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn prepare_image_is_idempotent_for_same_layer() {
        let server = MockServer::start().await;
        let cache = tempdir().unwrap();
        let registry = InstanceRegistry::new(None);
        let control = ControlPlane::with_image_cache_dir(registry.clone(), cache.path().into());

        let body = format!(
            r#"{{"image_ref":"{}/ns/image:tag","layers":[{{"index":0,"digest":"sha256:layer","size":4097,"media_type":"{}"}}],"fetch":{{"unit_bytes":1048576}},"pmem":{{"alignment_bytes":2097152}}}}"#,
            server.uri(),
            EROFS_LAYER_MEDIA_TYPE
        )
        .into_bytes();
        let first = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body: body.clone(),
            })
            .await
            .unwrap();
        let second = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body,
            })
            .await
            .unwrap();

        assert_eq!(registry.len().await, 1);
        assert_eq!(first.body, second.body);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn prepare_image_reuses_cache_for_same_digest_with_different_index() {
        let server = MockServer::start().await;
        let cache = tempdir().unwrap();
        let registry = InstanceRegistry::new(None);
        let control = ControlPlane::with_image_cache_dir(registry.clone(), cache.path().into());

        let first = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body: format!(
                    r#"{{"image_ref":"{}/ns/image:tag","layers":[{{"index":0,"digest":"sha256:layer","size":4097,"media_type":"{}"}}]}}"#,
                    server.uri(),
                    EROFS_LAYER_MEDIA_TYPE
                )
                .into_bytes(),
            })
            .await
            .unwrap();
        let second = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body: format!(
                    r#"{{"image_ref":"{}/ns/image:tag","layers":[{{"index":3,"digest":"sha256:layer","size":4097,"media_type":"{}"}}]}}"#,
                    server.uri(),
                    EROFS_LAYER_MEDIA_TYPE
                )
                .into_bytes(),
            })
            .await
            .unwrap();

        let first_value: serde_json::Value = serde_json::from_slice(&first.body).unwrap();
        let second_value: serde_json::Value = serde_json::from_slice(&second.body).unwrap();
        assert_eq!(registry.len().await, 1);
        assert_eq!(
            first_value["layers"][0]["sparse_path"],
            second_value["layers"][0]["sparse_path"]
        );
        assert_eq!(second_value["layers"][0]["index"], 3);
    }

    #[tokio::test]
    async fn prepare_kuasar_manifest_uses_described_content_identity() {
        const KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
        let dir = tempdir().unwrap();
        let socket = dir.path().join("accelerator.sock");
        let listener = SeqpacketListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let stream = listener.accept().unwrap();
                let request: serde_json::Value =
                    serde_json::from_slice(&stream.recv_packet().unwrap()).unwrap();
                assert_eq!(request["op"], "describe");
                assert_eq!(request["manifest_keys"], serde_json::json!([KEY]));
                let layout = sealed_layout_fd(4097, &[(0, 4097)]);
                stream
                    .send_packet_with_fd(
                        serde_json::to_string(&serde_json::json!({
                            "protocol_version": 1,
                            "request_id": request["request_id"],
                            "op": "describe_ok",
                            "content_id": format!("sha256:{}", "a".repeat(64)),
                            "image_size": 4097,
                            "layout_format": "kuasar-canonical-extents-v1",
                            "extent_count": 1,
                            "layout_size": 48
                        }))
                        .unwrap()
                        .as_bytes(),
                        layout.as_raw_fd(),
                    )
                    .unwrap();
            }
        });
        let cache = tempdir().unwrap();
        let registry = InstanceRegistry::new(None);
        let control = ControlPlane::with_image_cache_dir(registry.clone(), cache.path().into());

        let body = serde_json::to_vec(&serde_json::json!({
            "image_ref": format!("manifest://{KEY}"),
            "source": {
                "type": "kuasar-manifest",
                "manifest_keys": [KEY],
                "accelerator_socket": socket
            },
            "fetch": {"unit_bytes": 1048576},
            "pmem": {"alignment_bytes": 2097152}
        }))
        .unwrap();
        let response = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body: body.clone(),
            })
            .await
            .unwrap();
        let repeated = control
            .handle_request(Request {
                method: "POST".to_string(),
                path: "/api/v1/images/prepare".to_string(),
                body,
            })
            .await
            .unwrap();

        let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.body, repeated.body);
        assert_eq!(registry.len().await, 1);
        assert_eq!(value["layers"].as_array().unwrap().len(), 1);
        assert_eq!(value["layers"][0]["index"], 0);
        assert_eq!(
            value["layers"][0]["blob_digest"],
            format!("sha256:{}", "a".repeat(64))
        );
        assert_eq!(value["layers"][0]["blob_size"], 4097);
        assert_eq!(value["layers"][0]["pmem_size"], 2 * 1024 * 1024);
        assert_eq!(
            value["layers"][0]["media_type"],
            "application/vnd.erofs.image.v1"
        );
        server.join().unwrap();
    }

    #[test]
    fn parses_content_length() {
        assert_eq!(
            parse_headers(b"GET / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc")
                .unwrap()
                .unwrap(),
            (37, 3)
        );
    }

    #[test]
    fn renders_error_json() {
        let bytes = render_response(Err(Error::BadRequest(
            "target_path is required".to_string(),
        )))
        .unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.starts_with("HTTP/1.1 400 Bad Request"));
        assert!(text.contains("Content-Type: application/json"));
        assert!(text.ends_with("{\"error\":\"target_path is required\"}"));
    }
    fn sealed_layout_fd(image_size: u64, extents: &[(u64, u64)]) -> File {
        let mut data = vec![0; 32 + extents.len() * 16];
        data[..8].copy_from_slice(b"KCRANGE\0");
        data[8..12].copy_from_slice(&1u32.to_le_bytes());
        data[12..16].copy_from_slice(&16u32.to_le_bytes());
        data[16..24].copy_from_slice(&image_size.to_le_bytes());
        data[24..32].copy_from_slice(&(extents.len() as u64).to_le_bytes());
        for (index, (offset, len)) in extents.iter().copied().enumerate() {
            let pos = 32 + index * 16;
            data[pos..pos + 8].copy_from_slice(&offset.to_le_bytes());
            data[pos + 8..pos + 16].copy_from_slice(&len.to_le_bytes());
        }
        sealed_memfd(&data)
    }

    fn sealed_memfd(data: &[u8]) -> File {
        let name = CString::new("lazyd-control-test").unwrap();
        let fd = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        assert!(fd >= 0);
        let mut file = unsafe { File::from_raw_fd(fd) };
        file.write_all(data).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let seals =
            libc::F_SEAL_WRITE | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
        assert_eq!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) },
            0
        );
        file
    }
}
