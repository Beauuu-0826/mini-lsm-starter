pub mod txn;
pub mod watermark;

use std::{
    collections::{BTreeMap, HashSet},
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::lsm_storage::LsmStorageInner;

use self::{txn::Transaction, watermark::Watermark};

pub(crate) struct CommittedTxnData {
    pub(crate) key_hashes: HashSet<u32>,
    #[allow(dead_code)]
    pub(crate) read_ts: u64,
    #[allow(dead_code)]
    pub(crate) commit_ts: u64,
}

pub(crate) struct LsmMvccInner {
    pub(crate) write_lock: Mutex<()>,
    pub(crate) commit_lock: Mutex<()>,
    pub(crate) ts: Arc<Mutex<(u64, Watermark)>>,
    pub(crate) committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
}

impl LsmMvccInner {
    pub fn new(initial_ts: u64) -> Self {
        Self {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((initial_ts, Watermark::default()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn latest_commit_ts(&self) -> u64 {
        self.ts.lock().0
    }

    pub fn update_commit_ts(&self, ts: u64) {
        self.ts.lock().0 = ts;
    }

    /// All ts (strictly) below this ts can be garbage collected.
    pub fn watermark(&self) -> u64 {
        let ts = self.ts.lock();
        ts.1.watermark().unwrap_or(ts.0)
    }

    pub fn new_txn(&self, inner: Arc<LsmStorageInner>, serializable: bool) -> Arc<Transaction> {
        let read_ts = {
            let mut ts = self.ts.lock();
            let read_ts = ts.0;
            ts.1.add_reader(read_ts);
            read_ts
        };
        Arc::new(Transaction {
            read_ts,
            inner,
            local_storage: Arc::new(SkipMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            key_hashes: if serializable {
                Some(Mutex::new(Default::default()))
            } else {
                None
            },
        })
    }

    pub fn remove_reader(&self, read_ts: u64) {
        self.ts.lock().1.remove_reader(read_ts);
    }

    pub fn serializable(
        &self,
        read_ts: u64,
        expected_commit_ts: u64,
        key_hashes: &(HashSet<u32>, HashSet<u32>),
    ) -> bool {
        if key_hashes.0.is_empty() {
            return true;
        }
        for (_, transaction_write_set) in self.committed_txns.lock().range((
            Bound::Excluded(read_ts),
            Bound::Excluded(expected_commit_ts),
        )) {
            if !transaction_write_set.key_hashes.is_disjoint(&key_hashes.1) {
                return false;
            }
        }
        true
    }

    pub fn vacuum(&self) {
        let watermark = self.watermark();
        self.committed_txns
            .lock()
            .retain(|key, _| *key >= watermark);
    }

    pub fn maintain_commited_txn(&self, commit_ts: u64, read_ts: u64, write_set: HashSet<u32>) {
        self.committed_txns.lock().insert(
            commit_ts,
            CommittedTxnData {
                read_ts,
                commit_ts,
                key_hashes: write_set,
            },
        );
    }
}
