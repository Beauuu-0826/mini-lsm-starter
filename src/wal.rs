#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .create(true)
                    .open(path.as_ref())?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.as_ref())?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut buffer = &buffer[..];
        while buffer.has_remaining() {
            let body_size = buffer.get_u32() as usize;
            let mut body = &buffer.copy_to_bytes(body_size)[..];
            if crc32fast::hash(body) != buffer.get_u32() {
                bail!("{:?} wal has corrupted", path.as_ref());
            }
            while body.has_remaining() {
                let key_len = body.get_u16();
                let key = body.copy_to_bytes(key_len as usize);
                let ts = body.get_u64();
                let val_len = body.get_u16();
                let value = body.copy_to_bytes(val_len as usize);
                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut body = Vec::new();
        for (key, value) in data {
            body.put_u16(key.key_len() as u16);
            body.put(key.key_ref());
            body.put_u64(key.ts());
            body.put_u16(value.len() as u16);
            body.put(*value);
        }
        let mut encoded = Vec::new();
        encoded.put_u32(body.len() as u32);
        encoded.put(&body[..]);
        encoded.put_u32(crc32fast::hash(&body));
        self.file.lock().write_all(&encoded)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
