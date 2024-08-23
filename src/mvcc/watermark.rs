use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;

pub struct Watermark {
    readers: Arc<Mutex<BTreeMap<u64, usize>>>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers
            .lock()
            .entry(ts)
            .and_modify(|value| *value += 1)
            .or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let mut readers = self.readers.lock();
        if !readers.contains_key(&ts) {
            panic!("Can't remove an not exist reader");
        }
        readers.entry(ts).and_modify(|value| *value -= 1);
        if *readers.get(&ts).unwrap() == 0 {
            readers.remove(&ts);
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        Some(*self.readers.lock().first_key_value()?.0)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.lock().len()
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}
