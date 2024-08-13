#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }

    pub fn create_merge_iterator(
        &self,
        sst_ids: &[usize],
    ) -> Result<MergeIterator<SsTableIterator>> {
        let sstables = self.retrieve_sstable(sst_ids);
        let mut iters = Vec::with_capacity(sstables.len());
        for sst in sstables.into_iter() {
            iters.push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
        }
        Ok(MergeIterator::create(iters))
    }

    pub fn create_concat_iterator(&self, sst_ids: &[usize]) -> Result<SstConcatIterator> {
        SstConcatIterator::create_and_seek_to_first(self.retrieve_sstable(sst_ids))
    }

    fn retrieve_sstable(&self, sst_ids: &[usize]) -> Vec<Arc<SsTable>> {
        sst_ids
            .iter()
            .map(|sst_id| Arc::clone(self.sstables.get(sst_id).unwrap()))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // TODO no need to gain lock here?
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        if !self.inner.options.enable_wal {
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            }
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        fs::create_dir_all(path)?;
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1024));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let (manifest, records) = Manifest::recover(path.join("MANIFEST"))?;
        let mut need_load_ids = Vec::new();
        for record in records.into_iter() {
            match record {
                ManifestRecord::Compaction(task, mut sorted_run_ids) => {
                    let (new_state, remove_ids) = compaction_controller.apply_compaction_result(
                        &state,
                        &task,
                        &sorted_run_ids,
                        true,
                    );
                    need_load_ids.retain(|e| !remove_ids.contains(e));
                    need_load_ids.append(&mut sorted_run_ids);
                    state = new_state;
                }
                ManifestRecord::Flush(sst_id) => {
                    need_load_ids.push(sst_id);
                    if compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, sst_id);
                    } else {
                        state.levels.insert(0, (sst_id, vec![sst_id]));
                    }
                }
                _ => (),
            }
        }
        let max_used_id = need_load_ids.iter().copied().max().unwrap_or(0) + 1;
        state.memtable = Arc::new(MemTable::create(max_used_id));
        for sst_id in need_load_ids {
            let sst = SsTable::open(
                sst_id,
                Some(Arc::clone(&block_cache)),
                FileObject::open(&Self::path_of_sst_static(path, sst_id))?,
            )?;
            state.sstables.insert(sst_id, Arc::new(sst));
        }
        if let CompactionOptions::Leveled(_) = options.compaction_options {
            for idx in 0..state.levels.len() {
                let mut level_arrange = state.levels[idx].1.clone();
                level_arrange.sort_by(|a, b| {
                    state
                        .sstables
                        .get(a)
                        .unwrap()
                        .first_key()
                        .cmp(state.sstables.get(b).unwrap().first_key())
                });
                state.levels[idx].1 = level_arrange;
            }
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(max_used_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    /// Memtable -> Immutable Memtable -> L0 SsTable -> L1 SsTable
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = {
            let gaurd = self.state.read();
            Arc::clone(&gaurd)
        };
        if let Some(value) = state.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(value));
            }
        }
        for imm_memtable in state.imm_memtables.iter() {
            if let Some(value) = imm_memtable.get(key) {
                if value.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(value));
                }
            }
        }

        let mut sst_iters = Vec::with_capacity(state.l0_sstables.len());
        for sst in state.retrieve_sstable(&state.l0_sstables) {
            if sst.may_contain(KeyBytes::from_bytes(Bytes::copy_from_slice(key))) {
                sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(key),
                )?));
            }
        }

        let mut concat_iters = Vec::with_capacity(state.levels.len());
        for (_, sst_ids) in state.levels.iter() {
            concat_iters.push(Box::new(SstConcatIterator::create_and_seek_to_key(
                state
                    .retrieve_sstable(sst_ids)
                    .into_iter()
                    .filter(|sst| {
                        sst.may_contain(KeyBytes::from_bytes(Bytes::copy_from_slice(key)))
                    })
                    .collect(),
                KeySlice::from_slice(key),
            )?));
        }
        let iterator = TwoMergeIterator::create(
            MergeIterator::create(sst_iters),
            MergeIterator::create(concat_iters),
        )?;
        if iterator.is_valid() && iterator.key().raw_ref() == key && !iterator.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iterator.value())));
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let should_freeze = {
            let state = self.state.read();
            state.memtable.put(key, value)?;
            state
                .memtable
                .approximate_size()
                .gt(&self.options.target_sst_size)
        };
        if should_freeze {
            let state_lock = self.state_lock.lock();
            if self
                .state
                .read()
                .memtable
                .approximate_size()
                .gt(&self.options.target_sst_size)
            {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, b"")
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot
                .imm_memtables
                .insert(0, Arc::clone(&snapshot.memtable));
            snapshot.memtable = new_memtable;
            *guard = Arc::new(snapshot);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        if self.state.read().imm_memtables.is_empty() {
            return Ok(());
        }

        let imm_memtable = {
            let guard = self.state.read();
            Arc::clone(guard.imm_memtables.last().unwrap())
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        imm_memtable.flush(&mut sst_builder)?;
        let sst = sst_builder.build(
            imm_memtable.id(),
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(imm_memtable.id()),
        )?;
        {
            let lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst.sst_id());
            } else {
                snapshot
                    .levels
                    .insert(0, (sst.sst_id(), vec![sst.sst_id()]));
            }
            println!(
                "Flushing {}.sst with size={}",
                sst.sst_id(),
                sst.table_size()
            );
            snapshot.sstables.insert(sst.sst_id(), Arc::new(sst));
            *guard = Arc::new(snapshot);

            self.sync_dir()?;
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&lock, ManifestRecord::Flush(imm_memtable.id()))?;
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        // Memtable iterators
        let mut iters = Vec::with_capacity(state.imm_memtables.len() + 1);
        iters.push(Box::new(state.memtable.scan(lower, upper)));
        state
            .imm_memtables
            .iter()
            .for_each(|imm| iters.push(Box::new(imm.scan(lower, upper))));
        // l0 sst iterators
        let mut sst_iters = Vec::with_capacity(state.l0_sstables.len());
        for sst in state.retrieve_sstable(&state.l0_sstables) {
            if sst.range_overlap((map_bound(lower), map_bound(upper))) {
                sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
            }
        }
        // level concat iterators
        let mut concat_iters = Vec::with_capacity(state.levels.len());
        for (_, sst_ids) in state.levels.iter() {
            concat_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                state
                    .retrieve_sstable(sst_ids)
                    .into_iter()
                    .filter(|sst| sst.range_overlap((map_bound(lower), map_bound(upper))))
                    .collect(),
            )?));
        }

        let mut lsm_iterator = LsmIterator::new(
            TwoMergeIterator::create(
                MergeIterator::create(iters),
                TwoMergeIterator::create(
                    MergeIterator::create(sst_iters),
                    MergeIterator::create(concat_iters),
                )?,
            )?,
            map_bound(upper),
        )?;
        // consume the lsm iterator to meet the lower bound
        match lower {
            Bound::Unbounded => (),
            Bound::Included(key) => {
                while lsm_iterator.is_valid() && lsm_iterator.key().cmp(key) == Ordering::Less {
                    lsm_iterator.next()?;
                }
            }
            Bound::Excluded(key) => {
                while lsm_iterator.is_valid() && lsm_iterator.key().cmp(key) != Ordering::Greater {
                    lsm_iterator.next()?;
                }
            }
        }

        Ok(FusedIterator::new(lsm_iterator))
    }
}
