#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder};

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
        match _task {
            CompactionTask::ForceFullCompaction { l0_sstables, l1_sstables} => {
                self.build_sorted_run(&mut TwoMergeIterator::create(
                    state.create_merge_iterator(l0_sstables)?,
                    state.create_concat_iterator(l1_sstables)?,
                )?, _task.compact_to_bottom_level())
            },
            CompactionTask::Simple(task) => {
                if task.upper_level.is_none() {
                    return self.build_sorted_run(&mut TwoMergeIterator::create(
                        state.create_merge_iterator(&task.upper_level_sst_ids)?,
                        state.create_concat_iterator(&task.lower_level_sst_ids)?,
                    )?, _task.compact_to_bottom_level());
                }
                self.build_sorted_run(&mut TwoMergeIterator::create(
                    state.create_concat_iterator(&task.upper_level_sst_ids)?,
                    state.create_concat_iterator(&task.lower_level_sst_ids)?,
                )?, _task.compact_to_bottom_level())
            },
            CompactionTask::Tiered(task) => {
                let mut concat_iters = Vec::new();
                for (_, sst_ids) in task.tiers.iter(){
                    concat_iters.push(Box::new(state.create_concat_iterator(sst_ids)?));
                }
                self.build_sorted_run(
                    &mut MergeIterator::create(concat_iters),
                    _task.compact_to_bottom_level()
                )
            },
            CompactionTask::Leveled(task) => {
                if task.upper_level.is_none() {
                    return self.build_sorted_run(&mut TwoMergeIterator::create(
                        state.create_merge_iterator(&task.upper_level_sst_ids)?,
                        state.create_concat_iterator(&task.lower_level_sst_ids)?,
                    )?, _task.compact_to_bottom_level());
                }
                self.build_sorted_run(&mut TwoMergeIterator::create(
                    state.create_concat_iterator(&task.upper_level_sst_ids)?,
                    state.create_concat_iterator(&task.lower_level_sst_ids)?,
                )?, _task.compact_to_bottom_level())
            },
        }
    }

    fn build_sorted_run<I>(&self, iterator: &mut I, ignore_deleted: bool) -> Result<Vec<Arc<SsTable>>> 
    where I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>> {
        // consume the iterator adds key-value to sst_builder
        let mut sst_builder = Some(SsTableBuilder::new(self.options.block_size));
        let mut sorted_run = Vec::new();
        while iterator.is_valid() {
            if ignore_deleted && iterator.value().is_empty() {
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
        Ok(sorted_run)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let guard = self.state.read();
            let l0_sstables: Vec<usize> = guard.l0_sstables.to_vec();
            let l1_sstables: Vec<usize> = guard.levels[0].1.to_vec();
            (l0_sstables, l1_sstables)
        };
        let sorted_run = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;
        {
            let _lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                snapshot.sstables.remove(sst_id);
            }
            snapshot.l0_sstables.truncate(snapshot.l0_sstables.len() - l0_sstables.len());
            snapshot.levels.get_mut(0).unwrap().1.clear();
            for sst in sorted_run {
                snapshot.levels.get_mut(0).unwrap().1.push(sst.sst_id());
                snapshot.sstables.insert(sst.sst_id(), sst);
            }
            *guard = Arc::new(snapshot);
        }

        for table_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*table_id))?;
        }
        Ok(())
    }


    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self.compaction_controller
            .generate_compaction_task(&snapshot);
        if task.is_none() {
            return Ok(());
        }
        println!("Running compaction task: {:?}", task);
        let task = task.unwrap();
        let sorted_run = self.compact(&task)?;
        let sorted_run_ids: Vec<usize> = sorted_run.iter().map(|sst| sst.sst_id()).collect();

        let (lsm_storage_state, remove_ids) =
            self.compaction_controller.apply_compaction_result(&snapshot, &task, &sorted_run_ids, false);
        {
            let _lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            // maintain sstables
            for sst_id in remove_ids.iter() {
                snapshot.sstables.remove(sst_id);
            }
            for sst in sorted_run {
                snapshot.sstables.insert(sst.sst_id(), sst);
            }

            // update levels depend on compaction task
            match task {
                CompactionTask::Simple(_) => {
                    snapshot.l0_sstables.retain(|e| !remove_ids.contains(e));
                    snapshot.levels = lsm_storage_state.levels;
                },
                CompactionTask::Tiered(task) => {
                    let prev_tier_len = task.tiers.len() + lsm_storage_state.levels.len() - 1;
                    snapshot.levels.truncate(snapshot.levels.len() - prev_tier_len);
                    snapshot.levels.extend(lsm_storage_state.levels);
                },
                CompactionTask::Leveled(_) => {
                    snapshot.l0_sstables.retain(|e| !remove_ids.contains(e));
                    snapshot.levels = lsm_storage_state.levels;
                },
                _ => (),
            }
            *guard = Arc::new(snapshot);
        }

        println!("Compaction finished: {} files removed, {} files added", remove_ids.len(), sorted_run_ids.len());
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
