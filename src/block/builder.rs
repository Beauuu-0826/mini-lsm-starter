use std::cmp::min;

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

fn overlap_len(one: &[u8], another: &[u8]) -> usize {
    let check_len = min(one.len(), another.len());
    for i in 0..check_len {
        if one[i] != another[i] {
            return i;
        }
    }
    check_len
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
        let approximate_block_size = self.data.len()
            + self.offsets.len() * 2
            + 2
            + (key.key_len() - overlap_len(key.key_ref(), self.first_key.key_ref()))
            + value.len()
            + 18;
        if approximate_block_size.gt(&self.block_size) && !self.is_empty() {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        let overlap_len = overlap_len(self.first_key.key_ref(), key.key_ref());
        self.data.put_u16(overlap_len as u16);
        self.data.put_u16((key.key_len() - overlap_len) as u16);
        self.data.put(&key.key_ref()[overlap_len..]);
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        if self.first_key.is_empty() {
            self.first_key.append(key.key_ref());
        }
        self.last_key.set_from_slice(key);
        true
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
        KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(self.first_key.key_ref()),
            self.first_key.ts(),
        )
    }

    pub fn last_key(&self) -> KeyBytes {
        KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(self.last_key.key_ref()),
            self.last_key.ts(),
        )
    }
}
