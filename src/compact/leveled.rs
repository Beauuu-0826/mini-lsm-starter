use std::cmp::max;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_range(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> (usize, usize) {
        let key_range = {
            let (mut first_key, mut last_key) = {
                let sst = snapshot.sstables.get(&sst_ids[0]).unwrap();
                (sst.first_key().clone(), sst.last_key().clone())
            };
            for sst_id in sst_ids.iter().skip(1) {
                let sst = snapshot.sstables.get(sst_id).unwrap();
                if *sst.first_key() < first_key {
                    first_key = sst.first_key().clone();
                }
                if *sst.last_key() > last_key {
                    last_key = sst.last_key().clone();
                }
            }
            (first_key, last_key)
        };

        let mut start_index = None;
        let mut end_index = None;
        for (idx, sst_id) in snapshot.levels[in_level-1].1.iter().enumerate() {
            let sst = snapshot.sstables.get(sst_id).unwrap();
            if start_index.is_none() && *sst.last_key() >= key_range.0 {
                start_index = Some(idx);
            }
            if end_index.is_none() && *sst.first_key() > key_range.1 {
                end_index = Some(idx);
            }
        }
        (start_index.unwrap_or(0), end_index.unwrap_or(snapshot.levels[in_level-1].1.len()))
    }

    fn compute_target_level_size(&self, snapshot: &LsmStorageState) -> Vec<(usize, usize)> {
        let mut target_sizes: Vec<(usize, usize)> = Vec::with_capacity(snapshot.levels.len());
        target_sizes.push((self.options.max_levels, max(
            self.options.base_level_size_mb * 1024 * 1024,
            snapshot.levels[self.options.max_levels-1].1.iter()
                .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size() as usize).sum(),
        )));
        for (level, _) in snapshot.levels.iter().rev().skip(1) {
            if target_sizes[0].1 <= self.options.base_level_size_mb * 1024 * 1024 {
                target_sizes.insert(0, (*level, 0));
            } else {
                target_sizes.insert(0, (*level, target_sizes[0].1/self.options.level_size_multiplier));
            }
        }
        target_sizes
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let target_sizes = self.compute_target_level_size(snapshot);

        // l0 sst nums trigger compact
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let target = target_sizes.iter().skip_while(|target_size| target_size.1==0).next().unwrap();
            let overlap_range = self.find_overlapping_range(snapshot, &snapshot.l0_sstables, target.0);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: target.0,
                lower_level_sst_ids: snapshot.levels[target.0 - 1].1[overlap_range.0..overlap_range.1].to_vec(),
                is_lower_level_bottom_level: target.0 == self.options.max_levels,
            });
        }

        // priority trigger compact
        let result = snapshot.levels.iter().take(snapshot.levels.len()-1)
            .filter(|(level, _)| target_sizes[level-1].1!=0)
            .map(|(level, sst_ids)| {
                let level_size: usize = sst_ids.iter().map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size() as usize).sum();
                (*level, level_size as f64 / target_sizes[level-1].1 as f64)
            })
            .max_by(|x, y| x.1.total_cmp(&y.1))
            .unwrap_or((0, 0.0));
        if result.1 > 1.0 {
            // upper level sstable heuristic select
            let oldest_sst_id = *snapshot.levels[result.0 - 1].1.iter().min().unwrap();
            let overlap_range = self.find_overlapping_range(snapshot, &[oldest_sst_id], result.0 + 1);
            return Some(LeveledCompactionTask {
                upper_level: Some(result.0),
                upper_level_sst_ids: vec![oldest_sst_id],
                lower_level: result.0 + 1,
                lower_level_sst_ids: snapshot.levels[result.0].1[overlap_range.0..overlap_range.1].to_vec(),
                is_lower_level_bottom_level: result.0 + 1 == self.options.max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut remove_ids = Vec::new();
        remove_ids.extend(task.upper_level_sst_ids.iter());
        remove_ids.extend(task.lower_level_sst_ids.iter());
        let mut state = snapshot.clone();
        if task.upper_level.is_none() {
            state.l0_sstables.clear();
        } else {
            state.levels[task.upper_level.unwrap()-1].1.retain(|e| !remove_ids.contains(e));
        }
        
        state.levels[task.lower_level - 1].1 = {
            let mut low_level = Vec::new();
            let overlap_range = self.find_overlapping_range(snapshot, &task.upper_level_sst_ids, task.lower_level);
            low_level.extend(&snapshot.levels[task.lower_level - 1].1[0..overlap_range.0]);
            low_level.extend(output);
            low_level.extend(&snapshot.levels[task.lower_level - 1].1[overlap_range.1..]);
            low_level
        };
        (state, remove_ids)
    }
}
