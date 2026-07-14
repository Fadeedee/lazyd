use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::extent_layout::DataExtent;
use crate::range_map::{bitmap_path, range_is_present};
use crate::remote::BlobDescriptor;

const HEADER_SIZE: usize = 4096;
const MAGIC: &str = "lazyd-extent-map";
const VERSION: u32 = 1;
const EXTENT_ENTRY_SIZE: u64 = 16;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Header {
    magic: String,
    version: u32,
    blob_digest: String,
    blob_size: u64,
    extent_count: u64,
}

pub struct ExtentMap {
    file: File,
    blob_size: u64,
    extents: Arc<[DataExtent]>,
    slots_offset: u64,
    slots: Mutex<Vec<u8>>,
}

pub struct OpenedExtentMap {
    pub extent_map: ExtentMap,
    pub needs_recovery: bool,
}

impl ExtentMap {
    pub fn open_or_create(
        target_path: &Path,
        blob: &BlobDescriptor,
        extents: impl Into<Arc<[DataExtent]>>,
    ) -> Result<OpenedExtentMap> {
        let extents = extents.into();
        validate_extents(blob.size, &extents)?;
        let path = bitmap_path(target_path);
        let existed = path.exists();
        let expected = Header {
            magic: MAGIC.to_string(),
            version: VERSION,
            blob_digest: blob.digest.clone(),
            blob_size: blob.size,
            extent_count: extents.len() as u64,
        };
        let slots_offset = slots_offset(extents.len() as u64)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;

        let mut needs_recovery = false;
        let slots = if existed {
            match read_header(&mut file) {
                Ok(header) if header == expected => {
                    match (
                        read_extents(&file, header.extent_count),
                        read_slots(&file, slots_offset, header.extent_count),
                    ) {
                        (Ok(existing_extents), Ok(slots))
                            if existing_extents.as_slice() == extents.as_ref() =>
                        {
                            needs_recovery = true;
                            slots
                        }
                        _ => {
                            reset_sparse_cache(target_path)?;
                            initialize_file(&mut file, &expected, &extents, slots_offset)?;
                            vec![0; extents.len()]
                        }
                    }
                }
                _ => {
                    reset_sparse_cache(target_path)?;
                    initialize_file(&mut file, &expected, &extents, slots_offset)?;
                    vec![0; extents.len()]
                }
            }
        } else {
            reset_sparse_cache(target_path)?;
            initialize_file(&mut file, &expected, &extents, slots_offset)?;
            vec![0; extents.len()]
        };

        Ok(OpenedExtentMap {
            extent_map: Self {
                file,
                blob_size: blob.size,
                extents,
                slots_offset,
                slots: Mutex::new(slots),
            },
            needs_recovery,
        })
    }

    pub fn is_range_ready(&self, start: u64, len: u64) -> Result<bool> {
        let indexes = self.intersecting_extent_indexes(start, len)?;
        if indexes.is_empty() {
            return Ok(true);
        }
        let slots = self.slots.lock().unwrap();
        Ok(slots[indexes].iter().all(|ready| *ready != 0))
    }

    pub fn missing_extents(&self, start: u64, len: u64) -> Result<Vec<(usize, DataExtent)>> {
        let indexes = self.intersecting_extent_indexes(start, len)?;
        let slots = self.slots.lock().unwrap();
        Ok(indexes
            .filter(|index| slots[*index] == 0)
            .map(|index| (index, self.extents[index]))
            .collect())
    }

    pub fn set_extent_ready(&self, index: usize) -> Result<()> {
        if index >= self.extents.len() {
            return Err(Error::BadRequest(
                "extent index exceeds extent map".to_string(),
            ));
        }
        self.file
            .write_all_at(&[1], self.slots_offset + index as u64)?;
        self.file.sync_data()?;
        let mut slots = self.slots.lock().unwrap();
        slots[index] = 1;
        Ok(())
    }

    pub fn clear_extent_ready(&self, index: usize) -> Result<()> {
        if index >= self.extents.len() {
            return Err(Error::BadRequest(
                "extent index exceeds extent map".to_string(),
            ));
        }
        self.file
            .write_all_at(&[0], self.slots_offset + index as u64)?;
        self.file.sync_data()?;
        let mut slots = self.slots.lock().unwrap();
        slots[index] = 0;
        Ok(())
    }

    pub fn recovery_reconcile(&self, layer: &File) -> Result<()> {
        let ready = {
            let slots = self.slots.lock().unwrap();
            slots
                .iter()
                .enumerate()
                .filter_map(|(index, ready)| (*ready != 0).then_some(index))
                .collect::<Vec<_>>()
        };
        for index in ready {
            let extent = self.extents[index];
            if !range_is_present(layer, extent.offset, extent.len)? {
                self.clear_extent_ready(index)?;
            }
        }
        Ok(())
    }

    fn intersecting_extent_indexes(&self, start: u64, len: u64) -> Result<std::ops::Range<usize>> {
        if len == 0 {
            return Ok(0..0);
        }
        let end = start
            .checked_add(len)
            .ok_or_else(|| Error::BadRequest("range overflows u64".to_string()))?;
        if end > self.blob_size {
            return Err(Error::BadRequest("range exceeds blob size".to_string()));
        }
        let first = self
            .extents
            .partition_point(|extent| extent.offset + extent.len <= start);
        let count = self.extents[first..].partition_point(|extent| extent.offset < end);
        Ok(first..first + count)
    }
}

fn reset_sparse_cache(target_path: &Path) -> Result<()> {
    let target = OpenOptions::new()
        .read(true)
        .write(true)
        .open(target_path)?;
    let len = target.metadata()?.len();
    target.set_len(0)?;
    target.sync_all()?;
    target.set_len(len)?;
    target.sync_all()?;
    Ok(())
}

fn initialize_file(
    file: &mut File,
    header: &Header,
    extents: &[DataExtent],
    slots_offset: u64,
) -> Result<()> {
    file.set_len(slots_offset + extents.len() as u64)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&encode_header(header)?)?;
    write_extents(file, extents)?;
    if !extents.is_empty() {
        file.write_all(&vec![0; extents.len()])?;
    }
    file.sync_data()?;
    Ok(())
}

fn encode_header(header: &Header) -> Result<Vec<u8>> {
    let encoded = serde_json::to_vec(header)?;
    if encoded.len() + 1 > HEADER_SIZE {
        return Err(Error::BadRequest("extent map header too large".to_string()));
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
        .ok_or_else(|| Error::BadRequest("extent map header terminator missing".to_string()))?;
    if end == 0 {
        return Err(Error::BadRequest("extent map header is empty".to_string()));
    }
    Ok(serde_json::from_slice(&buf[..end])?)
}

fn write_extents(file: &mut File, extents: &[DataExtent]) -> Result<()> {
    for extent in extents {
        file.write_all(&extent.offset.to_le_bytes())?;
        file.write_all(&extent.len.to_le_bytes())?;
    }
    Ok(())
}

fn read_extents(file: &File, count: u64) -> Result<Vec<DataExtent>> {
    let mut bytes =
        vec![
            0;
            usize::try_from(count.checked_mul(EXTENT_ENTRY_SIZE).ok_or_else(|| {
                Error::BadRequest("extent table size overflows u64".to_string())
            },)?)
            .map_err(|_| Error::BadRequest("extent table size exceeds usize".to_string()))?
        ];
    if !bytes.is_empty() {
        file.read_exact_at(&mut bytes, HEADER_SIZE as u64)?;
    }
    let mut extents = Vec::with_capacity(count as usize);
    for chunk in bytes.chunks_exact(16) {
        let mut offset = [0u8; 8];
        let mut len = [0u8; 8];
        offset.copy_from_slice(&chunk[..8]);
        len.copy_from_slice(&chunk[8..16]);
        extents.push(DataExtent {
            offset: u64::from_le_bytes(offset),
            len: u64::from_le_bytes(len),
        });
    }
    Ok(extents)
}

fn read_slots(file: &File, slots_offset: u64, count: u64) -> Result<Vec<u8>> {
    let mut slots = vec![
        0;
        usize::try_from(count).map_err(|_| Error::BadRequest(
            "extent count exceeds usize".to_string()
        ))?
    ];
    if !slots.is_empty() {
        file.read_exact_at(&mut slots, slots_offset)?;
    }
    Ok(slots)
}

fn slots_offset(extent_count: u64) -> Result<u64> {
    extent_count
        .checked_mul(EXTENT_ENTRY_SIZE)
        .and_then(|size| size.checked_add(HEADER_SIZE as u64))
        .ok_or_else(|| Error::BadRequest("extent map size overflows u64".to_string()))
}

fn validate_extents(blob_size: u64, extents: &[DataExtent]) -> Result<()> {
    let mut previous_end = 0u64;
    for (index, extent) in extents.iter().enumerate() {
        if extent.len == 0 {
            return Err(Error::BadRequest(format!("extent {index} is empty")));
        }
        if extent.offset < previous_end {
            return Err(Error::BadRequest(format!(
                "extent {index} overlaps or is out of order"
            )));
        }
        let end = extent
            .offset
            .checked_add(extent.len)
            .ok_or_else(|| Error::BadRequest(format!("extent {index} overflows")))?;
        if end > blob_size {
            return Err(Error::BadRequest(format!(
                "extent {index} exceeds blob size"
            )));
        }
        previous_end = end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileExt;

    use super::*;
    use tempfile::NamedTempFile;

    fn blob(size: u64) -> BlobDescriptor {
        BlobDescriptor {
            digest: "sha256:abc".to_string(),
            size,
            media_type: None,
        }
    }

    #[test]
    fn range_readiness_follows_data_extents() {
        let file = NamedTempFile::new().unwrap();
        let extents = vec![
            DataExtent {
                offset: 0,
                len: 4096,
            },
            DataExtent {
                offset: 8192,
                len: 4096,
            },
        ];
        let opened = ExtentMap::open_or_create(file.path(), &blob(12288), extents).unwrap();

        assert!(opened.extent_map.is_range_ready(4096, 4096).unwrap());
        assert!(!opened.extent_map.is_range_ready(0, 1).unwrap());
        assert_eq!(
            opened.extent_map.missing_extents(0, 12288).unwrap().len(),
            2
        );

        opened.extent_map.set_extent_ready(0).unwrap();
        assert!(opened.extent_map.is_range_ready(0, 1).unwrap());
        assert!(!opened.extent_map.is_range_ready(0, 12288).unwrap());
    }

    #[test]
    fn persists_ready_slots() {
        let file = NamedTempFile::new().unwrap();
        let extents = vec![DataExtent {
            offset: 0,
            len: 4096,
        }];
        ExtentMap::open_or_create(file.path(), &blob(4096), extents.clone())
            .unwrap()
            .extent_map
            .set_extent_ready(0)
            .unwrap();

        let reopened = ExtentMap::open_or_create(file.path(), &blob(4096), extents).unwrap();

        assert!(reopened.needs_recovery);
        assert!(reopened.extent_map.is_range_ready(0, 4096).unwrap());
    }

    #[test]
    fn layout_change_rebuilds_empty_extent_map() {
        let file = NamedTempFile::new().unwrap();
        let first = vec![DataExtent {
            offset: 0,
            len: 4096,
        }];
        let opened = ExtentMap::open_or_create(file.path(), &blob(8192), first).unwrap();
        opened.extent_map.set_extent_ready(0).unwrap();
        drop(opened.extent_map);

        let second = vec![DataExtent {
            offset: 4096,
            len: 4096,
        }];
        let reopened = ExtentMap::open_or_create(file.path(), &blob(8192), second).unwrap();

        assert!(!reopened.needs_recovery);
        assert!(!reopened.extent_map.is_range_ready(4096, 4096).unwrap());
    }

    #[test]
    fn recovery_clears_ready_extent_missing_from_sparse_cache() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(4096).unwrap();
        let extents = vec![DataExtent {
            offset: 0,
            len: 4096,
        }];
        let opened = ExtentMap::open_or_create(file.path(), &blob(4096), extents).unwrap();
        opened.extent_map.set_extent_ready(0).unwrap();

        opened
            .extent_map
            .recovery_reconcile(file.as_file())
            .unwrap();

        assert!(!opened.extent_map.is_range_ready(0, 4096).unwrap());
    }

    #[test]
    fn new_extent_map_clears_stale_bytes_outside_data_extents() {
        let file = NamedTempFile::new().unwrap();
        file.as_file().set_len(8192).unwrap();
        file.as_file().write_all_at(&vec![0xaa; 4096], 0).unwrap();
        let extents = vec![DataExtent {
            offset: 4096,
            len: 4096,
        }];

        ExtentMap::open_or_create(file.path(), &blob(8192), extents).unwrap();

        let mut bytes = vec![0xff; 4096];
        file.as_file().read_exact_at(&mut bytes, 0).unwrap();
        assert!(bytes.iter().all(|byte| *byte == 0));
    }
}
