pub(crate) mod bloom;
mod builder;
mod iterator;

use std::cmp::Ordering;
use std::fs::File;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Error, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
use farmhash::fingerprint32;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut bytes = Vec::new();
        for meta_data in block_meta {
            bytes.put_u32(meta_data.offset as u32);
            bytes.put_u16(meta_data.first_key.key_len() as u16);
            bytes.put(meta_data.first_key.key_ref());
            bytes.put_u64(meta_data.first_key.ts());
            bytes.put_u16(meta_data.last_key.key_len() as u16);
            bytes.put(meta_data.last_key.key_ref());
            bytes.put_u64(meta_data.last_key.ts());
        }
        bytes.put_u32(crc32fast::hash(&bytes));
        buf.extend(bytes);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas = Vec::new();
        let mut buf = buf;
        let mut bytes = buf.copy_to_bytes(buf.remaining() - 4);
        if crc32fast::hash(&bytes) != buf.get_u32() {
            panic!("Block meta data has corrupted");
        }
        while bytes.has_remaining() {
            let offset = bytes.get_u32() as usize;
            let fkey_len = bytes.get_u16() as usize;
            let first_key = bytes.copy_to_bytes(fkey_len);
            let first_key_ts = bytes.get_u64();
            let lkey_len = bytes.get_u16() as usize;
            let last_key = bytes.copy_to_bytes(lkey_len);
            let last_key_ts = bytes.get_u64();
            metas.push(BlockMeta {
                offset,
                first_key: KeyBytes::from_bytes_with_ts(first_key, first_key_ts),
                last_key: KeyBytes::from_bytes_with_ts(last_key, last_key_ts),
            })
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let bloom_filter_offset = (&file.read(file.size() - 4, 4)?[..]).get_u32() as u64;
        let bloom_filter =
            Bloom::decode(&file.read(bloom_filter_offset, file.size() - 4 - bloom_filter_offset)?)?;

        let block_meta_offset = (&file.read(bloom_filter_offset - 4, 4)?[..]).get_u32() as u64;
        let block_meta = BlockMeta::decode_block_meta(
            &file.read(
                block_meta_offset,
                bloom_filter_offset - 4 - block_meta_offset,
            )?[..],
        );
        let (first_key, last_key) = (
            block_meta[0].first_key.clone(),
            block_meta[block_meta.len() - 1].last_key.clone(),
        );
        Ok(Self {
            file,
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            bail!("Block idx out of bound");
        }
        let start_index = self.block_meta[block_idx].offset as u64;
        let end_index = if block_idx + 1 == self.block_meta.len() {
            self.block_meta_offset
        } else {
            self.block_meta[block_idx + 1].offset
        } as u64;

        let mut bytes_with_checksum = &self.file.read(start_index, end_index - start_index)?[..];
        let block_bytes = bytes_with_checksum.copy_to_bytes(bytes_with_checksum.len() - 4);
        if crc32fast::hash(&block_bytes) != bytes_with_checksum.get_u32() {
            bail!("{}.sst' block has corrupted", self.id);
        }
        Ok(Arc::new(Block::decode(&block_bytes)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if self.block_cache.is_none() {
            return self.read_block(block_idx);
        }
        match self
            .block_cache
            .as_ref()
            .unwrap()
            .try_get_with((self.id, block_idx), || self.read_block(block_idx))
        {
            Err(_) => Err(Error::msg("Read block from file error")),
            Ok(result) => Ok(result),
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut start_index = 0;
        let mut end_index = self.block_meta.len() - 1;
        let mut idx_key = (0, KeySlice::from_slice(b"", TS_DEFAULT));
        while start_index <= end_index {
            idx_key = {
                let idx = (start_index + end_index) / 2;
                (idx, self.block_meta[idx].first_key.as_key_slice())
            };
            match key.cmp(&idx_key.1) {
                Ordering::Equal => break,
                Ordering::Greater => start_index = idx_key.0 + 1,
                Ordering::Less => {
                    if idx_key.0 == 0 {
                        break;
                    }
                    end_index = idx_key.0 - 1;
                }
            }
        }

        match key.cmp(&idx_key.1) {
            Ordering::Greater
                if key.cmp(&self.block_meta[idx_key.0].last_key.as_key_slice())
                    == Ordering::Greater
                    && idx_key.0 < self.block_meta.len() - 1 =>
            {
                idx_key.0 + 1
            }
            Ordering::Less
                if idx_key.0 != 0
                    && key.cmp(&self.block_meta[idx_key.0 - 1].last_key.as_key_slice())
                        != Ordering::Greater =>
            {
                idx_key.0 - 1
            }
            _ => idx_key.0,
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.first_key.key_ref() > key || self.last_key.key_ref() < key {
            return false;
        }
        return self.bloom.is_none()
            || self.bloom.as_ref().unwrap().may_contain(fingerprint32(key));
    }

    pub fn range_overlap<R>(&self, range: R) -> bool
    where
        R: RangeBounds<KeyBytes>,
    {
        range.contains(&self.first_key) || range.contains(&self.last_key)
    }
}
