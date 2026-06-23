use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::remote::BlobDescriptor;

const HEADER_SIZE: usize = 4096;
const MAGIC: &str = "lazyd-range-map";
const VERSION: u32 = 1;
pub const BITMAP_UNIT_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Header {
    magic: String,
    version: u32,
    unit_bytes: u64,
    blob_digest: String,
    blob_size: u64,
    slot_count: u64,
}

pub struct RangeMap {
    file: File,
    unit_bytes: u64,
    blob_size: u64,
    slot_count: u64,
    slots: Mutex<Vec<u8>>,
}

pub struct OpenedRangeMap {
    pub range_map: RangeMap,
    pub needs_recovery: bool,
}

impl RangeMap {
    pub fn open_or_create(target_path: &Path, blob: &BlobDescriptor) -> Result<OpenedRangeMap> {
        let unit_bytes = BITMAP_UNIT_BYTES;
        let path = bitmap_path(target_path);
        let existed = path.exists();
        let slot_count = slot_count(blob.size, unit_bytes)?;
        let expected = Header {
            magic: MAGIC.to_string(),
            version: VERSION,
            unit_bytes,
            blob_digest: blob.digest.clone(),
            blob_size: blob.size,
            slot_count,
        };

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut needs_recovery = false;
        let slots = if existed {
            match read_header(&mut file) {
                Ok(header) if header == expected => match read_slots(&file, slot_count) {
                    Ok(slots) => {
                        needs_recovery = true;
                        slots
                    }
                    Err(_) => {
                        initialize_file(&mut file, &expected)?;
                        vec![0; slot_count as usize]
                    }
                },
                _ => {
                    initialize_file(&mut file, &expected)?;
                    vec![0; slot_count as usize]
                }
            }
        } else {
            initialize_file(&mut file, &expected)?;
            vec![0; slot_count as usize]
        };

        Ok(OpenedRangeMap {
            range_map: Self {
                file,
                unit_bytes,
                blob_size: blob.size,
                slot_count,
                slots: Mutex::new(slots),
            },
            needs_recovery,
        })
    }

    pub fn is_range_ready(&self, start: u64, len: u64) -> bool {
        let Ok((slot_start, slot_end)) = self.slots_for_range(start, len) else {
            return false;
        };
        let slots = self.slots.lock().unwrap();
        slots[slot_start..slot_end].iter().all(|slot| *slot != 0)
    }

    pub fn set_range_ready(&self, start: u64, len: u64) -> Result<()> {
        self.update_range(start, len, 1)
    }

    pub fn clear_range_ready(&self, start: u64, len: u64) -> Result<()> {
        self.update_range(start, len, 0)
    }

    pub fn recovery_reconcile(&self, layer: &File) -> Result<()> {
        let ready_slots = {
            let slots = self.slots.lock().unwrap();
            slots
                .iter()
                .enumerate()
                .filter_map(|(index, ready)| (*ready != 0).then_some(index))
                .collect::<Vec<_>>()
        };

        for slot in ready_slots {
            let (offset, len) = self.slot_range(slot as u64)?;
            if !range_is_present(layer, offset, len)? {
                self.clear_range_ready(offset, len)?;
            }
        }
        Ok(())
    }

    fn update_range(&self, start: u64, len: u64, value: u8) -> Result<()> {
        let (slot_start, slot_end) = self.slots_for_range(start, len)?;
        if slot_start == slot_end {
            return Ok(());
        }

        let bytes = vec![value; slot_end - slot_start];
        self.file
            .write_all_at(&bytes, HEADER_SIZE as u64 + slot_start as u64)?;

        let mut slots = self.slots.lock().unwrap();
        slots[slot_start..slot_end].fill(value);
        Ok(())
    }

    fn slots_for_range(&self, start: u64, len: u64) -> Result<(usize, usize)> {
        if len == 0 {
            return Ok((0, 0));
        }
        let end = start
            .checked_add(len)
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        if end > self.blob_size {
            return Err(Error::BadRequest("range exceeds blob size".to_string()));
        }
        let slot_start = start / self.unit_bytes;
        let slot_end = end.div_ceil(self.unit_bytes);
        if slot_end > self.slot_count {
            return Err(Error::BadRequest("range exceeds slot map".to_string()));
        }
        Ok((slot_start as usize, slot_end as usize))
    }

    fn slot_range(&self, slot: u64) -> Result<(u64, u64)> {
        if slot >= self.slot_count {
            return Err(Error::BadRequest("slot exceeds range map".to_string()));
        }
        let offset = slot
            .checked_mul(self.unit_bytes)
            .ok_or_else(|| Error::BadRequest("slot offset overflows u64".to_string()))?;
        let end = (offset + self.unit_bytes).min(self.blob_size);
        Ok((offset, end - offset))
    }
}

pub fn bitmap_path(target_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.bitmap", target_path.display()))
}

pub fn validate_fetch_unit_bytes(unit_bytes: u64) -> Result<()> {
    if unit_bytes < BITMAP_UNIT_BYTES || unit_bytes % BITMAP_UNIT_BYTES != 0 {
        return Err(Error::BadRequest(
            "fetch unit_bytes must be a multiple of 1MiB and no smaller than 1MiB".to_string(),
        ));
    }
    Ok(())
}

pub fn range_is_present(file: &File, offset: u64, len: u64) -> Result<bool> {
    if len == 0 {
        return Ok(true);
    }
    let end = offset
        .checked_add(len)
        .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
    let data = unsafe { libc::lseek(file.as_raw_fd(), offset as libc::off_t, libc::SEEK_DATA) };
    if data < 0 {
        let err = std::io::Error::last_os_error();
        return match err.raw_os_error() {
            Some(libc::ENXIO) => Ok(false),
            Some(libc::EINVAL) => Ok(false),
            _ => Err(err.into()),
        };
    }
    if data as u64 > offset {
        return Ok(false);
    }
    let hole = unsafe { libc::lseek(file.as_raw_fd(), offset as libc::off_t, libc::SEEK_HOLE) };
    if hole < 0 {
        let err = std::io::Error::last_os_error();
        return match err.raw_os_error() {
            Some(libc::ENXIO) | Some(libc::EINVAL) => Ok(false),
            _ => Err(err.into()),
        };
    }
    Ok(hole as u64 >= end)
}

fn slot_count(blob_size: u64, unit_bytes: u64) -> Result<u64> {
    blob_size
        .checked_add(unit_bytes - 1)
        .map(|size| size / unit_bytes)
        .ok_or_else(|| Error::BadRequest("slot count overflows u64".to_string()))
}

fn initialize_file(file: &mut File, header: &Header) -> Result<()> {
    file.set_len(HEADER_SIZE as u64 + header.slot_count)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&encode_header(header)?)?;
    if header.slot_count > 0 {
        file.write_all(&vec![0; header.slot_count as usize])?;
    }
    file.sync_data()?;
    Ok(())
}

fn encode_header(header: &Header) -> Result<Vec<u8>> {
    let encoded = serde_json::to_vec(header)?;
    if encoded.len() + 1 > HEADER_SIZE {
        return Err(Error::BadRequest("range map header too large".to_string()));
    }
    let mut buf = vec![0; HEADER_SIZE];
    buf[..encoded.len()].copy_from_slice(&encoded);
    buf[encoded.len()] = b'\n';
    Ok(buf)
}

fn read_header(file: &mut File) -> Result<Header> {
    let mut buf = vec![0; HEADER_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;
    let end = buf
        .iter()
        .position(|byte| *byte == b'\n' || *byte == 0)
        .ok_or_else(|| Error::BadRequest("range map header terminator missing".to_string()))?;
    if end == 0 {
        return Err(Error::BadRequest("range map header is empty".to_string()));
    }
    Ok(serde_json::from_slice(&buf[..end])?)
}

fn read_slots(file: &File, slot_count: u64) -> Result<Vec<u8>> {
    let mut slots = vec![0; slot_count as usize];
    if !slots.is_empty() {
        file.read_exact_at(&mut slots, HEADER_SIZE as u64)?;
    }
    Ok(slots)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileExt;

    use tempfile::NamedTempFile;

    use super::*;

    fn blob(size: u64) -> BlobDescriptor {
        BlobDescriptor {
            digest: "sha256:abc".to_string(),
            size,
            media_type: None,
        }
    }

    #[test]
    fn range_map_sets_and_clears_slots() {
        let file = NamedTempFile::new().unwrap();
        let opened = RangeMap::open_or_create(file.path(), &blob(3 * 1024 * 1024)).unwrap();
        let map = opened.range_map;

        assert!(!map.is_range_ready(8, 4096));
        map.set_range_ready(8, 4096).unwrap();
        assert!(map.is_range_ready(0, 1024 * 1024));
        assert!(!map.is_range_ready(1024 * 1024, 1));

        map.clear_range_ready(8, 4096).unwrap();
        assert!(!map.is_range_ready(8, 4096));
    }

    #[test]
    fn header_mismatch_rebuilds_empty_map() {
        let file = NamedTempFile::new().unwrap();
        let opened = RangeMap::open_or_create(file.path(), &blob(1024 * 1024)).unwrap();
        opened.range_map.set_range_ready(0, 1).unwrap();
        drop(opened.range_map);

        let opened = RangeMap::open_or_create(file.path(), &blob(2 * 1024 * 1024)).unwrap();
        assert!(!opened.needs_recovery);
        assert!(!opened.range_map.is_range_ready(0, 1));
    }

    #[test]
    fn recovery_clears_ready_slot_when_layer_is_sparse() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(1024 * 1024).unwrap();
        let opened = RangeMap::open_or_create(file.path(), &blob(1024 * 1024)).unwrap();
        opened.range_map.set_range_ready(0, 1).unwrap();

        opened.range_map.recovery_reconcile(file.as_file()).unwrap();
        assert!(!opened.range_map.is_range_ready(0, 1));
    }

    #[test]
    fn recovery_keeps_ready_slot_when_layer_has_data() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(1024 * 1024).unwrap();
        file.as_file()
            .write_all_at(&vec![1; 1024 * 1024], 0)
            .unwrap();
        let opened = RangeMap::open_or_create(file.path(), &blob(1024 * 1024)).unwrap();
        opened.range_map.set_range_ready(0, 1).unwrap();

        opened.range_map.recovery_reconcile(file.as_file()).unwrap();
        assert!(opened.range_map.is_range_ready(0, 1));
    }
}
