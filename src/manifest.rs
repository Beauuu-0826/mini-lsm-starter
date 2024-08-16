use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Ok, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,

    path: PathBuf,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
    Snapshot(Vec<usize>, Vec<usize>, Vec<(usize, Vec<usize>)>),
}

impl Manifest {
    pub fn create(path: PathBuf) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(&path)?,
            )),
            path,
        })
    }

    pub fn recover(path: PathBuf) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = &buf[..];
        let mut manifest_records = Vec::new();
        while buf.has_remaining() {
            let len = buf.get_u16() as usize;
            let json_bytes = buf.copy_to_bytes(len);
            if crc32fast::hash(&json_bytes) != buf.get_u32() {
                bail!("{:?} manifest has corrupted", path);
            }
            manifest_records.push(
                serde_json::Deserializer::from_slice(&json_bytes)
                    .into_iter::<ManifestRecord>()
                    .next()
                    .unwrap()?,
            )
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
                path,
            },
            manifest_records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        if let ManifestRecord::Snapshot(..) = record {
            // truncate manifest
            let _ = OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&self.path)?;
        }

        let mut file = self.file.lock();
        let content = {
            let json_bytes = serde_json::to_vec(&record)?;
            let mut content = Vec::new();
            content.put_u16(json_bytes.len() as u16);
            content.put(&json_bytes[..]);
            content.put_u32(crc32fast::hash(&json_bytes));
            content
        };
        file.write_all(&content)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn size(&self) -> Result<usize> {
        Ok(self.file.lock().metadata()?.len() as usize)
    }
}
