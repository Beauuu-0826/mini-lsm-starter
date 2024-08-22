#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `KeyBytes` from a bound of `KeySlice`.
pub(crate) fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(x.into_key_bytes()),
        Bound::Excluded(x) => Bound::Excluded(x.into_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::new();
        let wal = Wal::recover(path, &map)?;
        Ok(Self {
            map: Arc::new(map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key, TS_RANGE_BEGIN)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        let lower = match lower {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_DEFAULT)),
            Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_DEFAULT)),
        };
        let upper = match upper {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_DEFAULT)),
            Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_DEFAULT)),
        };
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8], read_ts: u64) -> Option<Bytes> {
        let iterator = self.scan(
            Bound::Included(KeySlice::from_slice(key, read_ts)),
            Bound::Included(KeySlice::from_slice(key, TS_RANGE_END)),
        );
        if iterator.is_valid() {
            return Some(Bytes::copy_from_slice(iterator.value()));
        }
        None
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.put(key, value)?;
        }

        self.map
            .insert(key.into_key_bytes(), Bytes::copy_from_slice(value));
        self.approximate_size.fetch_add(
            key.raw_len() + value.len(),
            std::sync::atomic::Ordering::Release,
        );
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iterator = MemTableIteratorBuilder {
            map: Arc::clone(&self.map),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        iterator.next().unwrap();
        iterator
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key().as_key_slice(), entry.value());
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            let entry = fields.iter.next();
            match entry {
                None => *fields.item = (KeyBytes::new(), Bytes::new()),
                Some(entry) => {
                    fields.item.0 = entry.key().clone();
                    fields.item.1 = entry.value().clone();
                }
            };
        });
        Ok(())
    }
}
