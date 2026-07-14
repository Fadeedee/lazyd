use std::fs::File;
use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::instance::InstanceRegistry;

pub const PROTOCOL_VERSION: u32 = 1;
const MAX_PACKET_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRequest {
    pub protocol_version: u32,
    pub request_id: String,
    pub op: FetchOp,
    pub instance_id: String,
    pub pos: u64,
    pub len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchOp {
    Fetch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DataResponse {
    FetchOk {
        protocol_version: u32,
        request_id: String,
        ranges: Vec<FetchRange>,
    },
    Error {
        protocol_version: u32,
        request_id: String,
        code: u16,
        msg: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchRange {
    pub off: u64,
    pub len: u64,
    pub dev_off: u64,
}

pub struct SeqpacketListener {
    fd: OwnedFd,
}

pub struct SeqpacketStream {
    fd: OwnedFd,
}

#[derive(Clone)]
pub struct DataPlane {
    registry: InstanceRegistry,
}

impl DataPlane {
    pub fn new(registry: InstanceRegistry) -> Self {
        Self { registry }
    }

    pub fn bind(socket: &Path) -> Result<SeqpacketListener> {
        SeqpacketListener::bind(socket)
    }

    pub async fn serve_listener(&self, listener: SeqpacketListener) -> Result<()> {
        let listener = Arc::new(listener);
        loop {
            let listener = listener.clone();
            let stream = tokio::task::spawn_blocking(move || listener.accept())
                .await
                .map_err(|err| Error::Remote(err.to_string()))??;
            let data = self.clone();
            tokio::spawn(async move {
                if let Err(err) = data.handle_stream(stream).await {
                    tracing::warn!(%err, "data stream failed");
                }
            });
        }
    }

    async fn handle_stream(&self, stream: SeqpacketStream) -> Result<()> {
        loop {
            self.handle_stream_once(&stream).await?;
        }
    }

    pub async fn handle_stream_once(&self, stream: &SeqpacketStream) -> Result<()> {
        let packet = stream.recv_packet()?;
        let response = self.handle_packet(&packet).await;
        match response {
            Ok((response, fd)) => {
                let bytes = serde_json::to_vec(&response)?;
                stream.send_packet_with_fd(&bytes, fd.as_raw_fd())
            }
            Err(response) => {
                let bytes = serde_json::to_vec(&response)?;
                stream.send_packet(&bytes)
            }
        }
    }

    async fn handle_packet(
        &self,
        packet: &[u8],
    ) -> std::result::Result<(DataResponse, File), DataResponse> {
        let request = parse_fetch_request(packet)?;
        let request_id = request.request_id.clone();
        let result = self.handle_fetch(request).await;
        result.map_err(|err| DataResponse::Error {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            code: err.status_code(),
            msg: err.message(),
        })
    }

    async fn handle_fetch(&self, request: FetchRequest) -> Result<(DataResponse, File)> {
        let page_size = host_page_size()?;
        let instance = self
            .registry
            .get(&request.instance_id)
            .await
            .ok_or_else(|| Error::NotFound("instance not found".to_string()))?;
        let (range, fd) = instance
            .prepare_fetch_range(request.pos, request.len, page_size)
            .await?;
        Ok((
            DataResponse::FetchOk {
                protocol_version: PROTOCOL_VERSION,
                request_id: request.request_id,
                ranges: vec![range],
            },
            fd,
        ))
    }
}

fn parse_fetch_request(packet: &[u8]) -> std::result::Result<FetchRequest, DataResponse> {
    let request: FetchRequest =
        serde_json::from_slice(packet).map_err(|err| DataResponse::Error {
            protocol_version: PROTOCOL_VERSION,
            request_id: String::new(),
            code: 400,
            msg: err.to_string(),
        })?;
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(DataResponse::Error {
            protocol_version: PROTOCOL_VERSION,
            request_id: request.request_id,
            code: 400,
            msg: "unsupported protocol_version".to_string(),
        });
    }
    if request.request_id.is_empty() {
        return Err(DataResponse::Error {
            protocol_version: PROTOCOL_VERSION,
            request_id: request.request_id,
            code: 400,
            msg: "request_id is required".to_string(),
        });
    }
    Ok(request)
}

fn host_page_size() -> Result<u64> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(page_size as u64)
}

impl SeqpacketListener {
    pub fn bind(path: &Path) -> Result<Self> {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let fd = create_seqpacket_socket()?;
        let (addr, len) = sockaddr_un(path)?;
        let ret = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                &addr as *const libc::sockaddr_un as *const libc::sockaddr,
                len,
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let ret = unsafe { libc::listen(fd.as_raw_fd(), 128) };
        if ret < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self { fd })
    }

    pub fn accept(&self) -> Result<SeqpacketStream> {
        let fd = unsafe {
            libc::accept4(
                self.fd.as_raw_fd(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                libc::SOCK_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(SeqpacketStream {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        })
    }
}

impl SeqpacketStream {
    pub(crate) fn connect(path: &Path) -> Result<Self> {
        let fd = create_seqpacket_socket()?;
        let (addr, len) = sockaddr_un(path)?;
        let ret = unsafe {
            libc::connect(
                fd.as_raw_fd(),
                &addr as *const libc::sockaddr_un as *const libc::sockaddr,
                len,
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self { fd })
    }

    pub(crate) fn set_timeout(&self, timeout: Duration) -> Result<()> {
        if timeout.is_zero() {
            return Err(Error::BadRequest(
                "seqpacket timeout must be greater than zero".to_string(),
            ));
        }
        let seconds = libc::time_t::try_from(timeout.as_secs())
            .map_err(|_| Error::BadRequest("seqpacket timeout is too large".to_string()))?;
        let value = libc::timeval {
            tv_sec: seconds,
            tv_usec: timeout.subsec_micros().into(),
        };
        for option in [libc::SO_RCVTIMEO, libc::SO_SNDTIMEO] {
            let ret = unsafe {
                libc::setsockopt(
                    self.fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    option,
                    &value as *const libc::timeval as *const libc::c_void,
                    size_of::<libc::timeval>() as libc::socklen_t,
                )
            };
            if ret < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
        Ok(())
    }

    pub fn send_packet(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_PACKET_BYTES {
            return Err(Error::BadRequest("data packet too large".to_string()));
        }
        let sent = unsafe {
            libc::send(
                self.fd.as_raw_fd(),
                bytes.as_ptr() as *const libc::c_void,
                bytes.len(),
                libc::MSG_NOSIGNAL,
            )
        };
        if sent < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if sent as usize != bytes.len() {
            return Err(Error::Remote("short seqpacket send".to_string()));
        }
        Ok(())
    }

    pub fn send_packet_with_fd(&self, bytes: &[u8], fd: RawFd) -> Result<()> {
        if bytes.len() > MAX_PACKET_BYTES {
            return Err(Error::BadRequest("data packet too large".to_string()));
        }

        let mut iov = libc::iovec {
            iov_base: bytes.as_ptr() as *mut libc::c_void,
            iov_len: bytes.len(),
        };
        let mut control = vec![0u8; cmsg_space(size_of::<RawFd>())];
        let mut msg = unsafe { MaybeUninit::<libc::msghdr>::zeroed().assume_init() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = control.len();

        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            if cmsg.is_null() {
                return Err(Error::Remote(
                    "failed to build fd control message".to_string(),
                ));
            }
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as _) as _;
            std::ptr::copy_nonoverlapping(
                &fd as *const RawFd as *const u8,
                libc::CMSG_DATA(cmsg),
                size_of::<RawFd>(),
            );
            msg.msg_controllen = (*cmsg).cmsg_len;
        }

        let sent = unsafe { libc::sendmsg(self.fd.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) };
        if sent < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if sent as usize != bytes.len() {
            return Err(Error::Remote("short seqpacket send".to_string()));
        }
        Ok(())
    }

    pub fn recv_packet(&self) -> Result<Vec<u8>> {
        let (packet, _fd) = self.recv_packet_with_fd()?;
        Ok(packet)
    }

    pub fn recv_packet_with_fd(&self) -> Result<(Vec<u8>, Option<OwnedFd>)> {
        let mut buf = vec![0; MAX_PACKET_BYTES];
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };
        let mut control = vec![0u8; cmsg_space(size_of::<RawFd>())];
        let mut msg = unsafe { MaybeUninit::<libc::msghdr>::zeroed().assume_init() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = control.len();

        let received =
            unsafe { libc::recvmsg(self.fd.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
        if received < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if msg.msg_flags & libc::MSG_CTRUNC != 0 {
            return Err(Error::Remote("truncated fd control message".to_string()));
        }
        if msg.msg_flags & libc::MSG_TRUNC != 0 {
            return Err(Error::Remote("truncated seqpacket payload".to_string()));
        }
        let fd = unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            if cmsg.is_null() {
                None
            } else if (*cmsg).cmsg_level == libc::SOL_SOCKET
                && (*cmsg).cmsg_type == libc::SCM_RIGHTS
                && (*cmsg).cmsg_len >= libc::CMSG_LEN(size_of::<RawFd>() as _) as _
            {
                let mut fd = -1;
                std::ptr::copy_nonoverlapping(
                    libc::CMSG_DATA(cmsg),
                    &mut fd as *mut RawFd as *mut u8,
                    size_of::<RawFd>(),
                );
                (fd >= 0).then(|| OwnedFd::from_raw_fd(fd))
            } else {
                None
            }
        };
        if received == 0 {
            return Err(Error::Remote("data peer closed".to_string()));
        }
        buf.truncate(received as usize);
        Ok((buf, fd))
    }
}

fn cmsg_space(len: usize) -> usize {
    unsafe { libc::CMSG_SPACE(len as _) as usize }
}

fn create_seqpacket_socket() -> Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn sockaddr_un(path: &Path) -> Result<(libc::sockaddr_un, libc::socklen_t)> {
    let path = path
        .as_os_str()
        .as_encoded_bytes()
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let mut addr = unsafe { MaybeUninit::<libc::sockaddr_un>::zeroed().assume_init() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    if path.len() >= addr.sun_path.len() {
        return Err(Error::BadRequest("unix socket path too long".to_string()));
    }
    for (slot, byte) in addr.sun_path.iter_mut().zip(path) {
        *slot = byte as libc::c_char;
    }
    let len = (size_of::<libc::sa_family_t>() + path_len(&addr) + 1) as libc::socklen_t;
    Ok((addr, len))
}

fn path_len(addr: &libc::sockaddr_un) -> usize {
    addr.sun_path
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(addr.sun_path.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn fetch_request_schema_is_stable() {
        let request = FetchRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: "req-1".to_string(),
            op: FetchOp::Fetch,
            instance_id: "erofs-sha256-layer".to_string(),
            pos: 0,
            len: 4096,
        };

        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-1",
                "op": "fetch",
                "instance_id": "erofs-sha256-layer",
                "pos": 0,
                "len": 4096
            })
        );
    }

    #[test]
    fn fetch_ok_response_schema_is_stable() {
        let response = DataResponse::FetchOk {
            protocol_version: PROTOCOL_VERSION,
            request_id: "req-1".to_string(),
            ranges: vec![FetchRange {
                off: 0,
                len: 4096,
                dev_off: 0,
            }],
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-1",
                "op": "fetch_ok",
                "ranges": [
                    { "off": 0, "len": 4096, "dev_off": 0 }
                ]
            })
        );
    }

    #[test]
    fn error_response_schema_is_stable() {
        let response = DataResponse::Error {
            protocol_version: PROTOCOL_VERSION,
            request_id: "req-1".to_string(),
            code: 400,
            msg: "bad request".to_string(),
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-1",
                "op": "error",
                "code": 400,
                "msg": "bad request"
            })
        );
    }

    #[test]
    fn seqpacket_socket_preserves_packet_boundaries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("lazyd-data.sock");
        let listener = SeqpacketListener::bind(&path).unwrap();

        let server = std::thread::spawn(move || {
            let stream = listener.accept().unwrap();
            assert_eq!(stream.recv_packet().unwrap(), b"first");
            assert_eq!(stream.recv_packet().unwrap(), b"second");
        });

        let client = SeqpacketStream::connect(&path).unwrap();
        client.send_packet(b"first").unwrap();
        client.send_packet(b"second").unwrap();
        server.join().unwrap();
    }

    #[test]
    fn seqpacket_can_pass_file_descriptor() {
        use std::io::{Read, Seek, SeekFrom, Write};
        use std::os::fd::AsRawFd;
        use tempfile::NamedTempFile;

        let dir = tempdir().unwrap();
        let path = dir.path().join("lazyd-data.sock");
        let listener = SeqpacketListener::bind(&path).unwrap();
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"fd-data").unwrap();
        file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
        let fd = file.as_file().as_raw_fd();

        let server = std::thread::spawn(move || {
            let stream = listener.accept().unwrap();
            stream.send_packet_with_fd(b"ready", fd).unwrap();
        });

        let client = SeqpacketStream::connect(&path).unwrap();
        let (packet, fd) = client.recv_packet_with_fd().unwrap();
        assert_eq!(packet, b"ready");
        let mut received = std::fs::File::from(fd.unwrap());
        let mut text = String::new();
        received.read_to_string(&mut text).unwrap();
        assert_eq!(text, "fd-data");
        server.join().unwrap();
    }

    #[tokio::test]
    async fn fetch_request_returns_cache_fd() {
        use std::io::Read;
        use std::os::fd::AsRawFd;
        use tempfile::NamedTempFile;
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use crate::instance::{FetchConfig, InstanceConfig, InstanceRegistry, TriggerMode};
        use crate::remote::{BlobDescriptor, RemoteSource};

        let registry_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/ns/image/blobs/sha256:layer"))
            .and(header("range", "bytes=0-4095"))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(vec![b'x'; 4096]))
            .mount(&registry_server)
            .await;

        let cache = NamedTempFile::new().unwrap();
        cache.as_file().set_len(4096).unwrap();
        let registry = InstanceRegistry::new(None);
        registry
            .register(
                "inst".to_string(),
                InstanceConfig {
                    instance_id: String::new(),
                    target_path: cache.path().to_path_buf(),
                    blob: BlobDescriptor {
                        digest: "sha256:layer".to_string(),
                        size: 4096,
                        media_type: Some("application/vnd.erofs.layer.v1".to_string()),
                    },
                    source: RemoteSource::OciRegistry {
                        image_ref: format!("{}/ns/image:tag", registry_server.uri()),
                        hosts_dir: None,
                    },
                    auth: None,
                    fetch: FetchConfig {
                        unit_bytes: 1024 * 1024,
                    },
                    trigger_mode: TriggerMode::External,
                },
            )
            .await
            .unwrap();

        let dir = tempdir().unwrap();
        let socket = dir.path().join("lazyd-data.sock");
        let listener = SeqpacketListener::bind(&socket).unwrap();
        let data = DataPlane::new(registry);
        let server = tokio::spawn(async move {
            let stream = tokio::task::spawn_blocking(move || listener.accept())
                .await
                .unwrap()
                .unwrap();
            data.handle_stream_once(&stream).await.unwrap();
        });

        let response = tokio::task::spawn_blocking(move || {
            let client = SeqpacketStream::connect(&socket).unwrap();
            let request = serde_json::to_vec(&FetchRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "req-1".to_string(),
                op: FetchOp::Fetch,
                instance_id: "inst".to_string(),
                pos: 0,
                len: 4096,
            })
            .unwrap();
            client.send_packet(&request).unwrap();
            let (packet, fd) = client.recv_packet_with_fd().unwrap();
            let mut file = std::fs::File::from(fd.unwrap());
            let mut data = [0; 4];
            file.read_exact(&mut data).unwrap();
            (packet, data, file.as_raw_fd())
        })
        .await
        .unwrap();
        server.await.unwrap();

        let response_body: DataResponse = serde_json::from_slice(&response.0).unwrap();
        assert_eq!(
            response_body,
            DataResponse::FetchOk {
                protocol_version: PROTOCOL_VERSION,
                request_id: "req-1".to_string(),
                ranges: vec![FetchRange {
                    off: 0,
                    len: 4096,
                    dev_off: 0,
                }],
            }
        );
        assert_eq!(response.1, *b"xxxx");
        assert!(response.2 >= 0);
    }

    #[tokio::test]
    async fn fetch_tail_page_only_reads_real_blob_bytes_and_leaves_zero_padding() {
        use std::io::Read;
        use tempfile::NamedTempFile;
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use crate::instance::{FetchConfig, InstanceConfig, InstanceRegistry, TriggerMode};
        use crate::remote::{BlobDescriptor, RemoteSource};

        let page_size = host_page_size().unwrap() as usize;
        let blob_size = page_size - 17;
        let registry_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/ns/image/blobs/sha256:tail"))
            .and(header("range", format!("bytes=0-{}", blob_size - 1)))
            .respond_with(ResponseTemplate::new(206).set_body_bytes(vec![b'z'; blob_size]))
            .mount(&registry_server)
            .await;

        let cache = NamedTempFile::new().unwrap();
        cache.as_file().set_len(page_size as u64).unwrap();
        let registry = InstanceRegistry::new(None);
        registry
            .register(
                "tail".to_string(),
                InstanceConfig {
                    instance_id: String::new(),
                    target_path: cache.path().to_path_buf(),
                    blob: BlobDescriptor {
                        digest: "sha256:tail".to_string(),
                        size: blob_size as u64,
                        media_type: Some("application/vnd.erofs.layer.v1".to_string()),
                    },
                    source: RemoteSource::OciRegistry {
                        image_ref: format!("{}/ns/image:tag", registry_server.uri()),
                        hosts_dir: None,
                    },
                    auth: None,
                    fetch: FetchConfig {
                        unit_bytes: 1024 * 1024,
                    },
                    trigger_mode: TriggerMode::External,
                },
            )
            .await
            .unwrap();

        let dir = tempdir().unwrap();
        let socket = dir.path().join("lazyd-data.sock");
        let listener = SeqpacketListener::bind(&socket).unwrap();
        let data = DataPlane::new(registry);
        let server = tokio::spawn(async move {
            let stream = tokio::task::spawn_blocking(move || listener.accept())
                .await
                .unwrap()
                .unwrap();
            data.handle_stream_once(&stream).await.unwrap();
        });

        let page = tokio::task::spawn_blocking(move || {
            let client = SeqpacketStream::connect(&socket).unwrap();
            let request = serde_json::to_vec(&FetchRequest {
                protocol_version: PROTOCOL_VERSION,
                request_id: "tail-1".to_string(),
                op: FetchOp::Fetch,
                instance_id: "tail".to_string(),
                pos: 0,
                len: page_size as u64,
            })
            .unwrap();
            client.send_packet(&request).unwrap();
            let (_packet, fd) = client.recv_packet_with_fd().unwrap();
            let mut file = std::fs::File::from(fd.unwrap());
            let mut page = vec![0; page_size];
            file.read_exact(&mut page).unwrap();
            page
        })
        .await
        .unwrap();
        server.await.unwrap();

        assert!(page[..blob_size].iter().all(|byte| *byte == b'z'));
        assert!(page[blob_size..].iter().all(|byte| *byte == 0));
    }
}
