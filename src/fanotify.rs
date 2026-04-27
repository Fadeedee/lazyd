use std::collections::HashMap;
use std::ffi::CString;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::task;

use crate::error::{Error, Result};
use crate::instance::InstanceRegistry;

pub const FAN_PRE_ACCESS: u64 = 0x0010_0000;
pub const FAN_CLASS_PRE_CONTENT: u32 = 0x0000_0008;
pub const FAN_CLOEXEC: u32 = 0x0000_0001;
pub const FAN_NONBLOCK: u32 = 0x0000_0002;
pub const FAN_MARK_ADD: u32 = 0x0000_0001;
pub const FAN_MARK_REMOVE: u32 = 0x0000_0002;
pub const FAN_EVENT_INFO_TYPE_RANGE: u8 = 6;
pub const FAN_ALLOW: u32 = 0x01;
pub const FAN_DENY: u32 = 0x02;

const DEFAULT_ZERO_COUNT_WINDOW: u64 = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct FanotifyBackend {
    inner: Arc<FanotifyInner>,
}

struct FanotifyInner {
    fd: OwnedFd,
    marks: Mutex<HashMap<FileKey, String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileKey {
    dev: u64,
    ino: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanotifyRange {
    pub offset: u64,
    pub len: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FanotifyEventMetadata {
    event_len: u32,
    vers: u8,
    reserved: u8,
    metadata_len: u16,
    mask: u64,
    fd: i32,
    pid: i32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FanotifyEventInfoHeader {
    info_type: u8,
    pad: u8,
    len: u16,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FanotifyEventInfoRange {
    hdr: FanotifyEventInfoHeader,
    pad: u32,
    offset: u64,
    count: u64,
}

#[repr(C)]
struct FanotifyResponse {
    fd: i32,
    response: u32,
}

impl FanotifyBackend {
    pub fn new() -> Result<Self> {
        let flags = FAN_CLASS_PRE_CONTENT | FAN_CLOEXEC | FAN_NONBLOCK;
        let fd = unsafe {
            libc::syscall(
                libc::SYS_fanotify_init,
                flags,
                libc::O_RDONLY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(Error::Fanotify(std::io::Error::last_os_error().to_string()));
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd as RawFd) };
        Ok(Self {
            inner: Arc::new(FanotifyInner {
                fd,
                marks: Mutex::new(HashMap::new()),
            }),
        })
    }

    pub fn mark(&self, instance_id: String, path: &Path) -> Result<()> {
        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| Error::BadRequest("target_path contains nul byte".to_string()))?;
        let rc = unsafe {
            libc::syscall(
                libc::SYS_fanotify_mark,
                self.inner.fd.as_raw_fd(),
                FAN_MARK_ADD,
                FAN_PRE_ACCESS,
                libc::AT_FDCWD,
                c_path.as_ptr(),
            )
        };
        if rc < 0 {
            return Err(Error::Fanotify(std::io::Error::last_os_error().to_string()));
        }
        let metadata = std::fs::metadata(path)?;
        self.inner.marks.lock().unwrap().insert(
            FileKey {
                dev: metadata.dev(),
                ino: metadata.ino(),
            },
            instance_id,
        );
        Ok(())
    }

    pub fn unmark(&self, path: &Path) -> Result<()> {
        let metadata = std::fs::metadata(path)?;
        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| Error::BadRequest("target_path contains nul byte".to_string()))?;
        let rc = unsafe {
            libc::syscall(
                libc::SYS_fanotify_mark,
                self.inner.fd.as_raw_fd(),
                FAN_MARK_REMOVE,
                FAN_PRE_ACCESS,
                libc::AT_FDCWD,
                c_path.as_ptr(),
            )
        };
        if rc < 0 {
            return Err(Error::Fanotify(std::io::Error::last_os_error().to_string()));
        }
        self.inner.marks.lock().unwrap().remove(&FileKey {
            dev: metadata.dev(),
            ino: metadata.ino(),
        });
        Ok(())
    }

    pub fn spawn_event_loop(&self, registry: InstanceRegistry) {
        let backend = self.clone();
        task::spawn_blocking(move || backend.run_event_loop(registry));
    }

    fn run_event_loop(self, registry: InstanceRegistry) {
        let rt = tokio::runtime::Handle::current();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = unsafe {
                libc::read(
                    self.inner.fd.as_raw_fd(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            };
            if n < 0 {
                let err = std::io::Error::last_os_error();
                if matches!(err.raw_os_error(), Some(libc::EAGAIN) | Some(libc::EINTR)) {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                tracing::warn!(%err, "fanotify read failed");
                continue;
            }

            let events = parse_events(&buf[..n as usize]);
            for event in events {
                let Some(instance_id) = self.instance_id_for_fd(event.fd) else {
                    let _ = self.respond(event.fd, FAN_DENY);
                    close_event_fd(event.fd);
                    continue;
                };
                let allowed = rt.block_on(async {
                    let Some(instance) = registry.get(&instance_id).await else {
                        tracing::warn!(
                            instance_id,
                            offset = event.range.offset,
                            len = event.range.len,
                            "fanotify miss has no registered instance"
                        );
                        return false;
                    };
                    match instance
                        .ensure_range(event.range.offset, event.range.len)
                        .await
                    {
                        Ok(()) => true,
                        Err(err) => {
                            tracing::warn!(
                                %err,
                                instance_id,
                                offset = event.range.offset,
                                len = event.range.len,
                                "fanotify miss handling failed"
                            );
                            false
                        }
                    }
                });
                let response = if allowed { FAN_ALLOW } else { FAN_DENY };
                let _ = self.respond(event.fd, response);
                close_event_fd(event.fd);
            }
        }
    }

    fn instance_id_for_fd(&self, fd: RawFd) -> Option<String> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
        if rc < 0 {
            return None;
        }
        let stat = unsafe { stat.assume_init() };
        self.inner
            .marks
            .lock()
            .unwrap()
            .get(&FileKey {
                dev: stat.st_dev,
                ino: stat.st_ino,
            })
            .cloned()
    }

    fn respond(&self, fd: RawFd, response: u32) -> Result<()> {
        let response = FanotifyResponse { fd, response };
        let written = unsafe {
            libc::write(
                self.inner.fd.as_raw_fd(),
                &response as *const FanotifyResponse as *const libc::c_void,
                size_of::<FanotifyResponse>(),
            )
        };
        if written < 0 {
            return Err(Error::Fanotify(std::io::Error::last_os_error().to_string()));
        }
        Ok(())
    }
}

struct FanotifyEvent {
    fd: RawFd,
    range: FanotifyRange,
}

fn parse_events(buf: &[u8]) -> Vec<FanotifyEvent> {
    let mut events = Vec::new();
    let mut cursor = 0;
    while cursor + size_of::<FanotifyEventMetadata>() <= buf.len() {
        let metadata = read_unaligned::<FanotifyEventMetadata>(&buf[cursor..]);
        if metadata.event_len as usize > buf.len() - cursor
            || metadata.event_len as usize <= size_of::<FanotifyEventMetadata>()
        {
            break;
        }
        if metadata.mask & FAN_PRE_ACCESS != 0 {
            if let Some(range) = parse_range(
                &buf[cursor..cursor + metadata.event_len as usize],
                metadata.metadata_len as usize,
            ) {
                events.push(FanotifyEvent {
                    fd: metadata.fd,
                    range,
                });
            }
        }
        cursor += metadata.event_len as usize;
    }
    events
}

fn parse_range(event: &[u8], metadata_len: usize) -> Option<FanotifyRange> {
    let mut cursor = metadata_len;
    while cursor + size_of::<FanotifyEventInfoHeader>() <= event.len() {
        let header = read_unaligned::<FanotifyEventInfoHeader>(&event[cursor..]);
        let len = header.len as usize;
        if len < size_of::<FanotifyEventInfoHeader>() || cursor + len > event.len() {
            return None;
        }
        if header.info_type == FAN_EVENT_INFO_TYPE_RANGE
            && len >= size_of::<FanotifyEventInfoRange>()
        {
            let info = read_unaligned::<FanotifyEventInfoRange>(&event[cursor..]);
            let len = if info.count == 0 {
                DEFAULT_ZERO_COUNT_WINDOW
            } else {
                info.count
            };
            return Some(FanotifyRange {
                offset: info.offset,
                len,
            });
        }
        cursor += len;
    }
    None
}

fn read_unaligned<T: Copy>(bytes: &[u8]) -> T {
    assert!(bytes.len() >= size_of::<T>());
    unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const T) }
}

fn close_event_fd(fd: RawFd) {
    unsafe {
        libc::close(fd);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_match_linux_headers() {
        assert_eq!(FAN_PRE_ACCESS, 0x0010_0000);
        assert_eq!(FAN_CLASS_PRE_CONTENT, 0x0000_0008);
        assert_eq!(FAN_EVENT_INFO_TYPE_RANGE, 6);
    }

    #[test]
    fn parses_range_info() {
        let metadata_len = size_of::<FanotifyEventMetadata>();
        let event_len = metadata_len + size_of::<FanotifyEventInfoRange>();
        let metadata = FanotifyEventMetadata {
            event_len: event_len as u32,
            vers: 3,
            reserved: 0,
            metadata_len: metadata_len as u16,
            mask: FAN_PRE_ACCESS,
            fd: 9,
            pid: 1,
        };
        let range = FanotifyEventInfoRange {
            hdr: FanotifyEventInfoHeader {
                info_type: FAN_EVENT_INFO_TYPE_RANGE,
                pad: 0,
                len: size_of::<FanotifyEventInfoRange>() as u16,
            },
            pad: 0,
            offset: 4096,
            count: 8192,
        };
        let mut buf = Vec::new();
        append_struct(&mut buf, &metadata);
        append_struct(&mut buf, &range);

        let events = parse_events(&buf);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fd, 9);
        assert_eq!(
            events[0].range,
            FanotifyRange {
                offset: 4096,
                len: 8192
            }
        );
    }

    fn append_struct<T>(buf: &mut Vec<u8>, value: &T) {
        let bytes =
            unsafe { std::slice::from_raw_parts(value as *const T as *const u8, size_of::<T>()) };
        buf.extend_from_slice(bytes);
    }
}
