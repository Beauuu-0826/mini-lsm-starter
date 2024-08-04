use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashs: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashs: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashs.push(farmhash::fingerprint32(key.raw_ref()));
        if self.first_key.is_empty() {
            self.first_key.put(key.raw_ref());
        }
        self.last_key.clear();
        self.last_key.put(key.raw_ref());

        if self.builder.add(key, value) {
            return;
        }
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = Arc::new(builder.build());
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: block.key_at_index(0),
            last_key: block.key_at_index(block.offsets.len() - 1),
        });
        self.data.put(block.encode());
        let _ = self.builder.add(key, value);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut data = self.data;
        let mut meta = self.meta;
        if !self.builder.is_empty() {
            let block = Arc::new(self.builder.build());
            meta.push(BlockMeta {
                offset: data.len(),
                first_key: block.key_at_index(0),
                last_key: block.key_at_index(block.offsets.len() - 1),
            });
            data.put(block.encode());
        }

        let mut encoded = Vec::new();
        encoded.put(&data[..]);
        BlockMeta::encode_block_meta(&meta[..], &mut encoded);
        encoded.put_u32(data.len() as u32);

        let bloom_filter = Bloom::build_from_key_hashes(
            &self.key_hashs,
            Bloom::bloom_bits_per_key(self.key_hashs.len(), 0.01),
        );
        let bloom_filter_offset = encoded.len();
        bloom_filter.encode(&mut encoded);
        encoded.put_u32(bloom_filter_offset as u32);
        Ok(SsTable {
            file: FileObject::create(path.as_ref(), encoded)?,
            block_meta: meta,
            block_meta_offset: data.len(),
            id,
            block_cache,
            first_key: KeyVec::from_vec(self.first_key).into_key_bytes(),
            last_key: KeyVec::from_vec(self.last_key).into_key_bytes(),
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
