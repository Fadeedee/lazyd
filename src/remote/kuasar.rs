use std::fs::File;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::data::SeqpacketStream;
use crate::error::{Error, Result};
use crate::extent_layout::{
    CANONICAL_LAYOUT_FORMAT, DataExtent, ExtentLayout, MAX_CANONICAL_EXTENT_LENGTH,
    read_layout_file,
};
use crate::remote::{RemoteBackend, RemoteRange, RemoteSource};

const PROTOCOL_VERSION: u32 = 1;
pub const MAX_RANGE_LENGTH: u64 = MAX_CANONICAL_EXTENT_LENGTH;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const REQUIRED_MEMFD_SEALS: libc::c_int =
    libc::F_SEAL_WRITE | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RequestOp {
    Describe,
    ReadRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResponseOp {
    DescribeOk,
    ReadRangeOk,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DescribeRequest {
    protocol_version: u32,
    request_id: String,
    op: RequestOp,
    manifest_keys: Vec<String>,
}

impl DescribeRequest {
    fn new(request_id: String, manifest_keys: Vec<String>) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            op: RequestOp::Describe,
            manifest_keys,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DescribeResponse {
    protocol_version: u32,
    request_id: String,
    op: ResponseOp,
    content_id: String,
    image_size: u64,
    layout_format: String,
    extent_count: u64,
    layout_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRangeRequest {
    protocol_version: u32,
    request_id: String,
    op: RequestOp,
    manifest_keys: Vec<String>,
    offset: u64,
    length: u64,
}

impl ReadRangeRequest {
    fn new(request_id: String, manifest_keys: Vec<String>, offset: u64, length: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            op: RequestOp::ReadRange,
            manifest_keys,
            offset,
            length,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRangeResponse {
    protocol_version: u32,
    request_id: String,
    op: ResponseOp,
    offset: u64,
    length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorResponse {
    protocol_version: u32,
    request_id: String,
    op: ResponseOp,
    code: u16,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct ResponseHeader {
    protocol_version: u32,
    request_id: String,
    op: ResponseOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KuasarImage {
    pub content_id: String,
    pub image_size: u64,
    pub extents: Vec<DataExtent>,
}

#[derive(Debug, Clone)]
pub struct AcceleratorClient {
    socket: PathBuf,
    timeout: Duration,
}

impl AcceleratorClient {
    pub fn new(socket: PathBuf) -> Result<Self> {
        if socket.as_os_str().is_empty() {
            return Err(Error::BadRequest(
                "accelerator_socket is required".to_string(),
            ));
        }
        Ok(Self {
            socket,
            timeout: DEFAULT_REQUEST_TIMEOUT,
        })
    }

    #[cfg(test)]
    fn with_timeout(socket: PathBuf, timeout: Duration) -> Result<Self> {
        let mut client = Self::new(socket)?;
        if timeout.is_zero() {
            return Err(Error::BadRequest(
                "accelerator timeout must be greater than zero".to_string(),
            ));
        }
        client.timeout = timeout;
        Ok(client)
    }

    pub async fn describe(&self, manifest_keys: &[String]) -> Result<KuasarImage> {
        validate_manifest_keys(manifest_keys)?;
        let request_id = next_request_id();
        let request = DescribeRequest::new(request_id.clone(), manifest_keys.to_vec());
        let (payload, fd) = self.exchange(&request).await?;
        let response: DescribeResponse =
            parse_response(&payload, &request_id, ResponseOp::DescribeOk)?;
        if response.layout_format != CANONICAL_LAYOUT_FORMAT {
            return Err(Error::Remote(format!(
                "accelerator returned layout_format {}, expected {}",
                response.layout_format, CANONICAL_LAYOUT_FORMAT
            )));
        }
        let fd = fd.ok_or_else(|| {
            Error::Remote("accelerator describe response did not carry a layout fd".to_string())
        })?;
        let layout = read_layout_file(File::from(fd), response.layout_size)?;
        validate_layout_metadata(&response, &layout)?;
        if response
            .content_id
            .strip_prefix("sha256:")
            .is_none_or(|digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Err(Error::Remote(
                "accelerator returned an invalid content_id".to_string(),
            ));
        }
        if response.image_size == 0 {
            return Err(Error::Remote(
                "accelerator returned an empty image".to_string(),
            ));
        }
        Ok(KuasarImage {
            content_id: response.content_id,
            image_size: response.image_size,
            extents: layout.extents,
        })
    }

    pub async fn read_range(
        &self,
        manifest_keys: &[String],
        offset: u64,
        len: u64,
    ) -> Result<RemoteRange> {
        validate_manifest_keys(manifest_keys)?;
        validate_range(offset, len)?;
        let request_id = next_request_id();
        let request =
            ReadRangeRequest::new(request_id.clone(), manifest_keys.to_vec(), offset, len);
        let (payload, fd) = self.exchange(&request).await?;
        let response: ReadRangeResponse =
            parse_response(&payload, &request_id, ResponseOp::ReadRangeOk)?;
        if response.offset != offset || response.length != len {
            return Err(Error::Remote(format!(
                "accelerator returned range [{},{}) for requested [{},{})",
                response.offset,
                response.offset.saturating_add(response.length),
                offset,
                offset + len
            )));
        }
        let fd = fd.ok_or_else(|| {
            Error::Remote("accelerator read_range response did not carry an fd".to_string())
        })?;
        let file = validate_staging_file(fd, len)?;
        Ok(RemoteRange::StagingFile { file, len })
    }

    async fn exchange<T>(&self, request: &T) -> Result<(Vec<u8>, Option<OwnedFd>)>
    where
        T: Serialize,
    {
        let payload = serde_json::to_vec(request)?;
        let socket = self.socket.clone();
        let timeout = self.timeout;
        tokio::task::spawn_blocking(move || exchange_blocking(&socket, timeout, &payload))
            .await
            .map_err(|err| Error::Remote(format!("accelerator request task failed: {err}")))?
    }
}

pub struct KuasarRemoteBackend {
    client: AcceleratorClient,
    manifest_keys: Vec<String>,
}

impl KuasarRemoteBackend {
    pub fn from_source(source: &RemoteSource) -> Result<Self> {
        let RemoteSource::KuasarManifest {
            manifest_keys,
            accelerator_socket,
        } = source
        else {
            return Err(Error::BadRequest(
                "Kuasar backend requires a kuasar-manifest source".to_string(),
            ));
        };
        validate_manifest_keys(manifest_keys)?;
        Ok(Self {
            client: AcceleratorClient::new(PathBuf::from(accelerator_socket))?,
            manifest_keys: manifest_keys.clone(),
        })
    }
}

#[async_trait]
impl RemoteBackend for KuasarRemoteBackend {
    async fn read_range(&self, offset: u64, len: u64) -> Result<RemoteRange> {
        self.client
            .read_range(&self.manifest_keys, offset, len)
            .await
    }
}

fn exchange_blocking(
    socket: &Path,
    timeout: Duration,
    payload: &[u8],
) -> Result<(Vec<u8>, Option<OwnedFd>)> {
    let stream = SeqpacketStream::connect(socket)?;
    stream.set_timeout(timeout)?;
    stream.send_packet(payload)?;
    stream.recv_packet_with_fd()
}

fn parse_response<T>(payload: &[u8], request_id: &str, expected_op: ResponseOp) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let header: ResponseHeader = serde_json::from_slice(payload)?;
    if header.protocol_version != PROTOCOL_VERSION {
        return Err(Error::Remote(format!(
            "accelerator returned protocol_version {}",
            header.protocol_version
        )));
    }
    if header.request_id != request_id {
        return Err(Error::Remote(format!(
            "accelerator returned request_id {}, expected {}",
            header.request_id, request_id
        )));
    }
    if header.op == ResponseOp::Error {
        let response: ErrorResponse = serde_json::from_slice(payload)?;
        return Err(Error::Remote(format!(
            "accelerator error {}: {}",
            response.code, response.msg
        )));
    }
    if header.op != expected_op {
        return Err(Error::Remote(format!(
            "accelerator returned op {:?}, expected {:?}",
            header.op, expected_op
        )));
    }
    serde_json::from_slice(payload).map_err(Into::into)
}

fn validate_manifest_keys(manifest_keys: &[String]) -> Result<()> {
    if manifest_keys.is_empty() {
        return Err(Error::BadRequest(
            "manifest_keys must not be empty".to_string(),
        ));
    }
    for (index, key) in manifest_keys.iter().enumerate() {
        if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::BadRequest(format!(
                "manifest_keys[{index}] must be a bare 64-character hexadecimal key"
            )));
        }
    }
    Ok(())
}

fn validate_range(offset: u64, len: u64) -> Result<()> {
    if len == 0 {
        return Err(Error::BadRequest(
            "accelerator range length must be greater than zero".to_string(),
        ));
    }
    if len > MAX_RANGE_LENGTH {
        return Err(Error::BadRequest(format!(
            "accelerator range length {len} exceeds {MAX_RANGE_LENGTH}-byte limit"
        )));
    }
    offset
        .checked_add(len)
        .ok_or_else(|| Error::BadRequest("accelerator range overflows u64".to_string()))?;
    Ok(())
}

fn validate_staging_file(fd: OwnedFd, expected_len: u64) -> Result<File> {
    let file = File::from(fd);
    let actual_len = file.metadata()?.len();
    if actual_len != expected_len {
        return Err(Error::Remote(format!(
            "accelerator staging file is {actual_len} bytes, expected {expected_len}"
        )));
    }
    let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if seals < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if seals & REQUIRED_MEMFD_SEALS != REQUIRED_MEMFD_SEALS {
        return Err(Error::Remote(format!(
            "accelerator staging fd is missing required seals: got {seals:#x}"
        )));
    }
    Ok(file)
}

fn validate_layout_metadata(response: &DescribeResponse, layout: &ExtentLayout) -> Result<()> {
    if layout.image_size != response.image_size {
        return Err(Error::Remote(format!(
            "accelerator layout image_size {} does not match response {}",
            layout.image_size, response.image_size
        )));
    }
    if layout.extents.len() as u64 != response.extent_count {
        return Err(Error::Remote(format!(
            "accelerator layout extent_count {} does not match response {}",
            layout.extents.len(),
            response.extent_count
        )));
    }
    Ok(())
}

fn next_request_id() -> String {
    let sequence = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    format!("lazyd-{}-{sequence}", std::process::id())
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::fd::{AsRawFd, FromRawFd};

    use tempfile::tempdir;

    use crate::data::SeqpacketListener;
    use crate::remote::RemoteRange;

    use super::*;

    const KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn request_schemas_match_accelerator_v1() {
        let describe = DescribeRequest::new("req-1".to_string(), vec![KEY.to_string()]);
        assert_eq!(
            serde_json::to_value(describe).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-1",
                "op": "describe",
                "manifest_keys": [KEY]
            })
        );

        let read = ReadRangeRequest::new("req-2".to_string(), vec![KEY.to_string()], 4096, 8192);
        assert_eq!(
            serde_json::to_value(read).unwrap(),
            serde_json::json!({
                "protocol_version": 1,
                "request_id": "req-2",
                "op": "read_range",
                "manifest_keys": [KEY],
                "offset": 4096,
                "length": 8192
            })
        );
    }

    #[tokio::test]
    async fn describe_validates_response_identity_and_fd_count() {
        let dir = tempdir().unwrap();
        let socket = dir.path().join("accelerator.sock");
        let listener = SeqpacketListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let stream = listener.accept().unwrap();
            let request: DescribeRequest =
                serde_json::from_slice(&stream.recv_packet().unwrap()).unwrap();
            stream
                .send_packet_with_fd(
                    &serde_json::to_vec(&DescribeResponse {
                        protocol_version: PROTOCOL_VERSION,
                        request_id: request.request_id,
                        op: ResponseOp::DescribeOk,
                        content_id: format!("sha256:{}", "a".repeat(64)),
                        image_size: 16384,
                        layout_format: CANONICAL_LAYOUT_FORMAT.to_string(),
                        extent_count: 1,
                        layout_size: 48,
                    })
                    .unwrap(),
                    sealed_layout_fd(16384, &[(0, 16384)]).as_raw_fd(),
                )
                .unwrap();
        });

        let client = AcceleratorClient::new(socket).unwrap();
        let image = client.describe(&[KEY.to_string()]).await.unwrap();

        assert_eq!(image.content_id, format!("sha256:{}", "a".repeat(64)));
        assert_eq!(image.image_size, 16384);
        assert_eq!(
            image.extents,
            vec![DataExtent {
                offset: 0,
                len: 16384
            }]
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn read_range_receives_exact_sealed_staging_file() {
        let dir = tempdir().unwrap();
        let socket = dir.path().join("accelerator.sock");
        let listener = SeqpacketListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let stream = listener.accept().unwrap();
            let request: ReadRangeRequest =
                serde_json::from_slice(&stream.recv_packet().unwrap()).unwrap();
            let file = sealed_memfd(b"range-data");
            stream
                .send_packet_with_fd(
                    &serde_json::to_vec(&ReadRangeResponse {
                        protocol_version: PROTOCOL_VERSION,
                        request_id: request.request_id,
                        op: ResponseOp::ReadRangeOk,
                        offset: request.offset,
                        length: request.length,
                    })
                    .unwrap(),
                    file.as_raw_fd(),
                )
                .unwrap();
        });

        let client = AcceleratorClient::new(socket).unwrap();
        let range = client.read_range(&[KEY.to_string()], 0, 10).await.unwrap();
        let RemoteRange::StagingFile { mut file, len } = range else {
            panic!("accelerator returned in-memory bytes");
        };
        assert_ne!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let mut data = String::new();
        file.read_to_string(&mut data).unwrap();
        assert_eq!(len, 10);
        assert_eq!(data, "range-data");
        server.join().unwrap();
    }

    #[tokio::test]
    async fn read_range_propagates_remote_error() {
        let dir = tempdir().unwrap();
        let socket = dir.path().join("accelerator.sock");
        let listener = SeqpacketListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let stream = listener.accept().unwrap();
            let request: ReadRangeRequest =
                serde_json::from_slice(&stream.recv_packet().unwrap()).unwrap();
            stream
                .send_packet(
                    &serde_json::to_vec(&ErrorResponse {
                        protocol_version: PROTOCOL_VERSION,
                        request_id: request.request_id,
                        op: ResponseOp::Error,
                        code: 416,
                        msg: "range exceeds image".to_string(),
                    })
                    .unwrap(),
                )
                .unwrap();
        });

        let client = AcceleratorClient::new(socket).unwrap();
        let err = client
            .read_range(&[KEY.to_string()], 4096, 4096)
            .await
            .unwrap_err();

        assert!(err.to_string().contains("416"));
        assert!(err.to_string().contains("range exceeds image"));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn request_timeout_does_not_block_instance_fetch_forever() {
        let dir = tempdir().unwrap();
        let socket = dir.path().join("accelerator.sock");
        let listener = SeqpacketListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let stream = listener.accept().unwrap();
            let _request = stream.recv_packet().unwrap();
            std::thread::sleep(Duration::from_millis(100));
        });

        let client = AcceleratorClient::with_timeout(socket, Duration::from_millis(20)).unwrap();
        let err = client.describe(&[KEY.to_string()]).await.unwrap_err();

        assert!(matches!(err, Error::Io(_)));
        server.join().unwrap();
    }

    fn sealed_memfd(data: &[u8]) -> File {
        let name = CString::new("lazyd-kuasar-test").unwrap();
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
}
