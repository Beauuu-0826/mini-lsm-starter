use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if _snapshot.l0_sstables.len().ge(&self.options.level0_file_num_compaction_trigger) {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: 1 == self.options.max_levels,
            });
        }
        for level_id in 0..self.options.max_levels - 1 {
            if (_snapshot.levels[level_id + 1].1.len() as f64
                / _snapshot.levels[level_id].1.len() as f64) * 100.0 < self.options.size_ratio_percent as f64 {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level_id + 1),
                    upper_level_sst_ids: _snapshot.levels[level_id].1.clone(),
                    lower_level: level_id + 2,
                    lower_level_sst_ids: _snapshot.levels[level_id + 1].1.clone(),
                    is_lower_level_bottom_level: level_id + 2 == self.options.max_levels,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut lsm_storage_state = _snapshot.clone();
        if _task.upper_level.is_none() {
            lsm_storage_state.l0_sstables
                .truncate(lsm_storage_state.l0_sstables.len() - _task.upper_level_sst_ids.len());
        } else {
            lsm_storage_state.levels[_task.upper_level.unwrap() - 1].1.clear();
        }
        lsm_storage_state.levels[_task.lower_level - 1].1.clear();
        lsm_storage_state.levels[_task.lower_level - 1].1.extend(_output);

        let mut remove_sst_id = Vec::new();
        remove_sst_id.extend(_task.upper_level_sst_ids.iter());
        remove_sst_id.extend(_task.lower_level_sst_ids.iter());
        (lsm_storage_state, remove_sst_id)
    }
}
