use std::path::PathBuf;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::error::{Error, Result};
use crate::instance::{InstanceConfig, InstanceRegistry};

#[derive(Clone)]
pub struct ControlPlane {
    registry: InstanceRegistry,
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
    io_backend: &'static str,
    remote_backend: &'static str,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl ControlPlane {
    pub fn new(registry: InstanceRegistry) -> Self {
        Self { registry }
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
                    io_backend: "fanotify",
                    remote_backend: "oci-registry",
                })?;
                Ok(Response::json(200, body))
            }
            _ if request.method == "PUT" && request.path.starts_with("/api/v1/instances/") => {
                let instance_id = request
                    .path
                    .trim_start_matches("/api/v1/instances/")
                    .to_string();
                if instance_id.is_empty() {
                    return Err(Error::BadRequest("instance id is required".to_string()));
                }
                let mut config: InstanceConfig = serde_json::from_slice(&request.body)?;
                config.instance_id = instance_id.clone();
                self.registry.register(instance_id, config).await?;
                Ok(Response::empty(204))
            }
            _ if request.method == "DELETE" && request.path.starts_with("/api/v1/instances/") => {
                let instance_id = request.path.trim_start_matches("/api/v1/instances/");
                self.registry.unregister(instance_id).await?;
                Ok(Response::empty(204))
            }
            _ => Err(Error::NotFound("unknown endpoint".to_string())),
        }
    }
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
    use super::*;
    use crate::instance::InstanceRegistry;

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
        assert_eq!(value["io_backend"], "fanotify");
        assert_eq!(value["remote_backend"], "oci-registry");
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
}
