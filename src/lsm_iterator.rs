use std::{cmp::Ordering, ops::Bound};

use anyhow::{bail, Result};

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    key::KeyBytes,
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    MergeIterator<MemTableIterator>,
    TwoMergeIterator<MergeIterator<SsTableIterator>, MergeIterator<SstConcatIterator>>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,

    end_bound: Bound<KeyBytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<KeyBytes>) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            end_bound,
        };
        while iter.is_valid() && iter.value().is_empty() {
            iter.next()?;
        }
        Ok(iter)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }
        match &self.end_bound {
            Bound::Included(key_bytes) => {
                self.inner.key().into_key_bytes().cmp(key_bytes) != Ordering::Greater
            }
            Bound::Excluded(key_bytes) => {
                self.inner.key().into_key_bytes().cmp(key_bytes) == Ordering::Less
            }
            _ => true,
        }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        let mut prev_key = self.inner.key().into_key_bytes();
        self.inner.next()?;
        while self.inner.is_valid()
            && (self.inner.value().is_empty() || self.inner.key().key_ref() == prev_key.key_ref())
        {
            prev_key = self.inner.key().into_key_bytes();
            self.inner.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("Can't access the iterator when it's invalid");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Can't access the iterator when it's invalid");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Error case");
        }
        if !self.is_valid() {
            return Ok(());
        }
        let result = self.iter.next();
        if result.is_err() {
            self.has_errored = true;
        }
        result
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
