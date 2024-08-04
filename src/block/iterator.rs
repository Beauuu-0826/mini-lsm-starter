use std::{cmp::Ordering, sync::Arc};

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        let first_key_len = (&iter.block.data[2..4]).get_u16() as usize;
        iter.first_key
            .append(&iter.block.data[4..4 + first_key_len]);
        iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_first();
        iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iterator = Self::new(block);
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        if self.block.offsets.is_empty() {
            return;
        }
        self.seek_to_index(0);
        self.idx += 1;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }
        self.seek_to_index(self.idx);
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if self.block.offsets.is_empty() {
            return;
        }

        let mut start_index = 0;
        let mut end_index = self.block.offsets.len() - 1;
        while start_index <= end_index {
            self.seek_to_index((start_index + end_index) / 2);
            match self.key().cmp(&key) {
                Ordering::Equal => break,
                Ordering::Less => start_index = self.idx + 1,
                Ordering::Greater => {
                    if self.idx == 0 {
                        break;
                    }
                    end_index = self.idx - 1;
                }
            }
        }
        self.idx += 1;
        if self.key().cmp(&key) == Ordering::Less {
            self.next();
        }
    }

    fn seek_to_index(&mut self, index: usize) {
        let start_index = self.block.offsets[index] as usize;
        let end_index = self
            .block
            .offsets
            .get(index + 1)
            .copied()
            .map(|end| end as usize)
            .unwrap_or_else(|| self.block.data.len());
        let (overlap_len, rest_len, val_len) = {
            let overlap_len = (&self.block.data[start_index..start_index + 2]).get_u16() as usize;
            let rest_len = (&self.block.data[start_index + 2..start_index + 4]).get_u16() as usize;
            let val_len = (&self.block.data[start_index + 4 + rest_len..start_index + 6 + rest_len])
                .get_u16() as usize;
            (overlap_len, rest_len, val_len)
        };
        self.key.set_from_slice(KeySlice::from_slice(
            &self.first_key.raw_ref()[0..overlap_len],
        ));
        self.key
            .append(&self.block.data[start_index + 4..start_index + 4 + rest_len]);
        self.value_range = (end_index - val_len, end_index);
        self.idx = index;
    }
}
