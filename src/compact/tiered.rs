use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // space amplification ratio trigger compact, using file nums to esimate size
        let estimate_engine_size = snapshot
            .levels
            .iter()
            .map(|level| level.1.len())
            .sum::<usize>();
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        if (estimate_engine_size - last_level_size) as f64 / last_level_size as f64
            >= self.options.max_size_amplification_percent as f64 / 100.0
        {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // size ratio trigger component
        let mut accumulate_size = 0;
        for (idx, (_, level)) in snapshot.levels.iter().enumerate() {
            if idx + 1 >= self.options.min_merge_width
                && (accumulate_size as f64 / level.len() as f64)
                    >= (100.0 + self.options.size_ratio as f64) / 100.0
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[..idx + 1].to_vec(),
                    bottom_tier_included: idx + 1 == snapshot.levels.len(),
                });
            }
            accumulate_size += level.len();
        }

        // final tier nums trigger compact
        if snapshot.levels.len() > self.options.num_tiers {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels[..snapshot.levels.len() + 1 - self.options.num_tiers]
                    .to_vec(),
                bottom_tier_included: self.options.num_tiers == 1,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();
        let mut remove_ids = Vec::new();
        for (_, ids) in task.tiers.iter() {
            remove_ids.extend(ids.iter());
        }
        state.levels = state.levels.into_iter().skip(task.tiers.len()).collect();
        state.levels.insert(0, (output[0], output.to_vec()));
        (state, remove_ids)
    }
}
