#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::borrow::BorrowMut;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let state = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut iterator = match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // create l0 sstable iterators
                let mut l0_sst_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        Arc::clone(state.sstables.get(id).unwrap()),
                    )?));
                }
                // retrieve l1 sstable
                let mut l1_sst = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_sst.push(Arc::clone(state.sstables.get(id).unwrap()));
                }
                // use concat iterator to create merge iterator
                TwoMergeIterator::create(
                    MergeIterator::create(l0_sst_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_sst)?,
                )?
            }
            _ => unimplemented!(),
        };

        // consume the iterator adds key-value to sst_builder
        let mut sst_builder = Some(SsTableBuilder::new(self.options.block_size));
        let mut sorted_run = Vec::new();
        while iterator.is_valid() {
            // In ForceFullCompaction, those deleted key doesn't need to be written into sst file
            if iterator.value().is_empty() {
                iterator.next()?;
                continue;
            }
            let builder_inner = sst_builder.as_mut().unwrap();
            builder_inner.add(iterator.key(), iterator.value());
            if builder_inner
                .estimated_size()
                .gt(&self.options.target_sst_size)
            {
                let sst_id = self.next_sst_id();
                sorted_run.push(Arc::new(sst_builder.take().unwrap().build(
                    sst_id,
                    Some(Arc::clone(&self.block_cache)),
                    self.path_of_sst(sst_id),
                )?));
                sst_builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            iterator.next()?;
        }

        let sst_id = self.next_sst_id();
        sorted_run.push(Arc::new(sst_builder.take().unwrap().build(
            sst_id,
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(sst_id),
        )?));
        return Ok(sorted_run);
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let guard = self.state.read();
            let l0_sstables: Vec<usize> = guard.l0_sstables.iter().map(|id| *id).collect();
            let l1_sstables: Vec<usize> = guard.levels[0].1.iter().map(|id| *id).collect();
            (l0_sstables, l1_sstables)
        };
        let sorted_run = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;
        {
            let _lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let state = Arc::make_mut(guard.borrow_mut());
            for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                state.sstables.remove(sst_id);
            }
            state
                .l0_sstables
                .truncate(state.l0_sstables.len() - l0_sstables.len());
            state.levels.get_mut(0).unwrap().1.clear();
            for sst in sorted_run {
                let sst_id = sst.sst_id();
                state.levels.get_mut(0).unwrap().1.push(sst_id);
                state.sstables.insert(sst_id, sst);
            }
        }

        for table_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*table_id))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let (lsm_storage_state, remove_ids, sorted_run) = {
            let snapshort = self.state.read();
            let task = self
                .compaction_controller
                .generate_compaction_task(&snapshort);
            if task.is_none() {
                return Ok(());
            }
            let sorted_run = self.compact(&task.as_ref().unwrap())?;
            let sorted_run_ids: Vec<usize> = sorted_run.iter().map(|sst| sst.sst_id()).collect();
            let (lsm_storage_state, remove_ids) =
                self.compaction_controller.apply_compaction_result(
                    &snapshort,
                    task.as_ref().unwrap(),
                    &sorted_run_ids,
                    false,
                );
            (lsm_storage_state, remove_ids, sorted_run)
        };
        {
            let _lock = self.state_lock.lock();
            let mut lsm_storage_state = lsm_storage_state;
            let mut guard = self.state.write();
            for sst_id in remove_ids.iter() {
                lsm_storage_state.sstables.remove(sst_id);
            }
            for sst in sorted_run {
                lsm_storage_state.sstables.insert(sst.sst_id(), sst);
            }
            *guard = Arc::new(lsm_storage_state);
        }

        for sst_id in remove_ids.into_iter() {
            std::fs::remove_file(self.path_of_sst(sst_id))?;
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() + 1 > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
