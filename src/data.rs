use std::mem::{MaybeUninit, size_of};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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
    #[cfg(test)]
    fn connect(path: &Path) -> Result<Self> {
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

    pub fn recv_packet(&self) -> Result<Vec<u8>> {
        let mut buf = vec![0; MAX_PACKET_BYTES];
        let received = unsafe {
            libc::recv(
                self.fd.as_raw_fd(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
                0,
            )
        };
        if received < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if received == 0 {
            return Err(Error::Remote("data peer closed".to_string()));
        }
        buf.truncate(received as usize);
        Ok(buf)
    }
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
}
