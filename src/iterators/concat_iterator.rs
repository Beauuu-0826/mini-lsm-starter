use std::{cmp::Ordering, sync::Arc};

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let current =
            SsTableIterator::create_and_seek_to_first(Arc::clone(sstables.get(0).unwrap()))?;
        Ok(Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut pick = sstables.len();
        for (idx, sst) in sstables.iter().enumerate() {
            if sst.last_key().raw_ref().cmp(key.raw_ref()) != Ordering::Less {
                pick = idx;
                break;
            }
        }
        if pick == sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: pick,
                sstables,
            });
        }
        let current =
            SsTableIterator::create_and_seek_to_key(Arc::clone(sstables.get(pick).unwrap()), key)?;
        Ok(Self {
            current: Some(current),
            next_sst_idx: pick + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }
        self.current.as_mut().unwrap().next()?;
        if !self.current.as_ref().unwrap().is_valid() && self.next_sst_idx < self.sstables.len() - 1
        {
            self.current = Some(SsTableIterator::create_and_seek_to_first(Arc::clone(
                &self.sstables[self.next_sst_idx],
            ))?);
            self.next_sst_idx += 1;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
