use bytes::{BufMut, Bytes};

use crate::key::{KeyBytes, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// The last key in the block
    last_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::default(),
            last_key: KeyVec::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.is_empty() {
            self.first_key.append(key.raw_ref());
            self.last_key.set_from_slice(key);
            self.append_data(key, value);
            return true;
        }
        let approximate_block_size =
            self.data.len() + self.offsets.len() * 2 + key.len() + value.len() + 8;
        if approximate_block_size.gt(&self.block_size) {
            return false;
        }
        self.last_key.set_from_slice(key);
        self.append_data(key, value);
        true
    }

    fn append_data(&mut self, key: KeySlice, value: &[u8]) {
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key.raw_ref());
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn first_key(&self) -> KeyBytes {
        KeyBytes::from_bytes(Bytes::copy_from_slice(self.first_key.raw_ref()))
    }

    pub fn last_key(&self) -> KeyBytes {
        KeyBytes::from_bytes(Bytes::copy_from_slice(self.last_key.raw_ref()))
    }
}
