use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::{Deref, DerefMut};

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I> Deref for HeapWrapper<I>
where
    I: StorageIterator,
{
    type Target = I;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl<I: StorageIterator> DerefMut for HeapWrapper<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.1
    }
}

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iterators = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().filter(|iter| iter.is_valid()).enumerate() {
            iterators.push(HeapWrapper(idx, iter));
        }
        let current = iterators.pop();
        Self {
            iters: iterators,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        match self.current.as_ref() {
            Some(current) if current.is_valid() => true,
            _ => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();

        while self.iters.peek().is_some() {
            let mut peek = self.iters.peek_mut().unwrap();
            if peek.key() != current.key() {
                break;
            }
            let result = peek.next();
            if result.is_err() {
                PeekMut::pop(peek);
                return result;
            }
            if !peek.is_valid() {
                PeekMut::pop(peek);
            }
        }

        current.next()?;
        if current.is_valid() {
            self.iters.push(self.current.take().unwrap());
        }
        self.current = self.iters.pop();
        Ok(())
    }
}
