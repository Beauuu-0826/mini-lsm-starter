use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::{bail, Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

pub(crate) type RwSet = (
    HashSet<Bytes>,
    HashSet<Bytes>,
    HashSet<(Bound<Bytes>, Bound<Bytes>)>,
);

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) rw_set: Option<Mutex<RwSet>>,
}

fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("This transaction has entered the commit phase");
        }
        if let Some(ref guard) = self.rw_set {
            guard.lock().1.insert(Bytes::copy_from_slice(key));
        }

        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }
        self.inner.get(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("This transaction has entered the commit phase");
        }
        if let Some(ref guard) = self.rw_set {
            guard.lock().2.insert((map_bound(lower), map_bound(upper)));
        }

        let mut local_iterator = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        local_iterator.next()?;
        let storage_iterator = self.inner.scan(lower, upper, self.read_ts)?;
        Ok(TxnIterator {
            _txn: self.clone(),
            iter: TwoMergeIterator::create(local_iterator, storage_iterator)?,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("This transaction has entered the commit phase");
        }
        if let Some(ref guard) = self.rw_set {
            guard.lock().0.insert(Bytes::copy_from_slice(key));
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, b"");
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .expect("The transaction has entered the commit phase");

        // To ensure that only one transaction can enter into commit phase at a time
        let mvcc = self.inner.mvcc.as_ref().unwrap();
        let _commit_lock = mvcc.commit_lock.lock();
        if let Some(ref guard) = self.rw_set {
            if !mvcc.serializable(self.read_ts, mvcc.latest_commit_ts() + 1, &guard.lock()) {
                bail!("This transaction may embrace the serializable isolation, can't commit");
            }
        }

        let batch_record = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        self.inner.write_batch(&batch_record)?;

        if let Some(ref guard) = self.rw_set {
            mvcc.maintain_commited_txn(
                mvcc.latest_commit_ts(),
                self.read_ts,
                guard.lock().0.clone(),
            );
            mvcc.vacuum();
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner
            .mvcc
            .as_ref()
            .unwrap()
            .remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|fields| {
            let entry = fields.iter.next();
            match entry {
                None => *fields.item = (Bytes::new(), Bytes::new()),
                Some(entry) => {
                    fields.item.0 = entry.key().clone();
                    fields.item.1 = entry.value().clone();
                }
            };
        });
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { _txn: txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
