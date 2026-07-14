use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const CANONICAL_LAYOUT_FORMAT: &str = "kuasar-canonical-extents-v1";
pub const MAX_CANONICAL_EXTENT_LENGTH: u64 = 64 << 20;
const MAGIC: &[u8; 8] = b"KCRANGE\0";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 32;
const ENTRY_SIZE: usize = 16;
const MAX_LAYOUT_SIZE: u64 = 64 << 20;
const REQUIRED_MEMFD_SEALS: libc::c_int =
    libc::F_SEAL_WRITE | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataExtent {
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtentLayout {
    pub image_size: u64,
    pub extents: Vec<DataExtent>,
}

impl ExtentLayout {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(Error::Remote(
                "accelerator layout fd is shorter than header".to_string(),
            ));
        }
        if &bytes[..8] != MAGIC {
            return Err(Error::Remote(
                "accelerator layout fd has invalid magic".to_string(),
            ));
        }
        let version = read_u32(&bytes[8..12]);
        if version != VERSION {
            return Err(Error::Remote(format!(
                "accelerator layout version {version} is unsupported"
            )));
        }
        let entry_size = read_u32(&bytes[12..16]);
        if entry_size != ENTRY_SIZE as u32 {
            return Err(Error::Remote(format!(
                "accelerator layout entry size {entry_size} is unsupported"
            )));
        }
        let image_size = read_u64(&bytes[16..24]);
        let count = read_u64(&bytes[24..32]);
        let count_usize = usize::try_from(count).map_err(|_| {
            Error::Remote("accelerator layout extent count is too large".to_string())
        })?;
        let expected = HEADER_SIZE
            .checked_add(count_usize.checked_mul(ENTRY_SIZE).ok_or_else(|| {
                Error::Remote("accelerator layout size overflows usize".to_string())
            })?)
            .ok_or_else(|| Error::Remote("accelerator layout size overflows usize".to_string()))?;
        if bytes.len() != expected {
            return Err(Error::Remote(format!(
                "accelerator layout fd size {} does not match expected {expected}",
                bytes.len()
            )));
        }

        let mut extents = Vec::with_capacity(count_usize);
        let mut pos = HEADER_SIZE;
        for _ in 0..count_usize {
            extents.push(DataExtent {
                offset: read_u64(&bytes[pos..pos + 8]),
                len: read_u64(&bytes[pos + 8..pos + 16]),
            });
            pos += ENTRY_SIZE;
        }
        validate_extents(image_size, &extents)?;
        Ok(Self {
            image_size,
            extents,
        })
    }
}

pub fn read_layout_file(file: std::fs::File, expected_size: u64) -> Result<ExtentLayout> {
    if expected_size > MAX_LAYOUT_SIZE {
        return Err(Error::Remote(format!(
            "accelerator layout size {expected_size} exceeds {MAX_LAYOUT_SIZE}-byte limit"
        )));
    }
    let actual_size = file.metadata()?.len();
    if actual_size != expected_size {
        return Err(Error::Remote(format!(
            "accelerator layout fd is {actual_size} bytes, expected {expected_size}"
        )));
    }
    // SAFETY: F_GET_SEALS only inspects the open descriptor and does not
    // dereference userspace memory.
    let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if seals < 0 || seals & REQUIRED_MEMFD_SEALS != REQUIRED_MEMFD_SEALS {
        return Err(Error::Remote(
            "accelerator layout fd must be a sealed memfd".to_string(),
        ));
    }
    let mut bytes = vec![0; expected_size as usize];
    file.read_exact_at(&mut bytes, 0)?;
    ExtentLayout::decode(&bytes)
}

fn validate_extents(image_size: u64, extents: &[DataExtent]) -> Result<()> {
    let mut previous_end = 0u64;
    for (index, extent) in extents.iter().enumerate() {
        if extent.len == 0 {
            return Err(Error::Remote(format!(
                "accelerator layout extent {index} is empty"
            )));
        }
        if extent.len > MAX_CANONICAL_EXTENT_LENGTH {
            return Err(Error::Remote(format!(
                "accelerator layout extent {index} length {} exceeds {MAX_CANONICAL_EXTENT_LENGTH}-byte range limit",
                extent.len
            )));
        }
        if extent.offset < previous_end {
            return Err(Error::Remote(format!(
                "accelerator layout extent {index} overlaps or is out of order"
            )));
        }
        let end = extent
            .offset
            .checked_add(extent.len)
            .ok_or_else(|| Error::Remote(format!("accelerator layout extent {index} overflows")))?;
        if end > image_size {
            return Err(Error::Remote(format!(
                "accelerator layout extent {index} exceeds image size"
            )));
        }
        previous_end = end;
    }
    Ok(())
}

fn read_u32(bytes: &[u8]) -> u32 {
    let mut value = [0u8; 4];
    value.copy_from_slice(bytes);
    u32::from_le_bytes(value)
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut value = [0u8; 8];
    value.copy_from_slice(bytes);
    u64::from_le_bytes(value)
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};

    use tempfile::tempfile;

    use super::*;

    fn encode(image_size: u64, extents: &[DataExtent]) -> Vec<u8> {
        let mut bytes = vec![0; HEADER_SIZE + extents.len() * ENTRY_SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..12].copy_from_slice(&VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&(ENTRY_SIZE as u32).to_le_bytes());
        bytes[16..24].copy_from_slice(&image_size.to_le_bytes());
        bytes[24..32].copy_from_slice(&(extents.len() as u64).to_le_bytes());
        for (index, extent) in extents.iter().enumerate() {
            let pos = HEADER_SIZE + index * ENTRY_SIZE;
            bytes[pos..pos + 8].copy_from_slice(&extent.offset.to_le_bytes());
            bytes[pos + 8..pos + 16].copy_from_slice(&extent.len.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn decodes_canonical_layout() {
        let bytes = encode(
            8193,
            &[
                DataExtent {
                    offset: 0,
                    len: 4096,
                },
                DataExtent {
                    offset: 8192,
                    len: 1,
                },
            ],
        );

        let layout = ExtentLayout::decode(&bytes).unwrap();

        assert_eq!(layout.image_size, 8193);
        assert_eq!(layout.extents.len(), 2);
        assert_eq!(layout.extents[1].offset, 8192);
    }

    #[test]
    fn rejects_bad_layout() {
        let bad_magic = vec![0; HEADER_SIZE];
        assert!(ExtentLayout::decode(&bad_magic).is_err());

        let overlap = encode(
            10,
            &[
                DataExtent { offset: 0, len: 8 },
                DataExtent { offset: 4, len: 1 },
            ],
        );
        assert!(ExtentLayout::decode(&overlap).is_err());

        let oversized = encode(
            (64 << 20) + 1,
            &[DataExtent {
                offset: 0,
                len: (64 << 20) + 1,
            }],
        );
        assert!(ExtentLayout::decode(&oversized).is_err());
    }

    #[test]
    fn rejects_unsealed_layout_file() {
        let bytes = encode(
            4096,
            &[DataExtent {
                offset: 0,
                len: 4096,
            }],
        );
        let mut file = tempfile().unwrap();
        file.write_all(&bytes).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        let error = read_layout_file(file, bytes.len() as u64).unwrap_err();

        assert!(error.to_string().contains("sealed memfd"), "{error}");
    }

    #[test]
    fn rejects_oversized_layout_before_reading_fd() {
        let file = tempfile().unwrap();

        let error = read_layout_file(file, (64 << 20) + 1).unwrap_err();

        assert!(error.to_string().contains("exceeds"), "{error}");
    }
}
