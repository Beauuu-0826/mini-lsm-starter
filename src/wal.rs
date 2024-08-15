#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.as_ref())?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let mut buffer = &buffer[..];
        while buffer.has_remaining() {
            let key_len = buffer.get_u16();
            let key = buffer.copy_to_bytes(key_len as usize);
            let val_len = buffer.get_u16();
            let value = buffer.copy_to_bytes(val_len as usize);
            let checksum_buf = {
                let mut checksum_buf = Vec::new();
                checksum_buf.put_u16(key_len);
                checksum_buf.put(key.clone());
                checksum_buf.put_u16(val_len);
                checksum_buf.put(value.clone());
                checksum_buf
            };
            if crc32fast::hash(&checksum_buf) != buffer.get_u32() {
                bail!("{:?} wal has corrupted", path.as_ref());
            }
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut buffer = Vec::new();
        buffer.put_u16(key.len() as u16);
        buffer.put(key);
        buffer.put_u16(value.len() as u16);
        buffer.put(value);
        buffer.put_u32(crc32fast::hash(&buffer));
        let mut file = self.file.lock();
        file.write_all(&buffer)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
