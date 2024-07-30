mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut encoded: Vec<u8> = Vec::new();
        encoded.put(&self.data[..]);
        self.offsets
            .iter()
            .for_each(|offset| encoded.put_u16(*offset));
        encoded.put_u16(self.offsets.len() as u16);
        Bytes::copy_from_slice(&encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - 2..]).get_u16() as usize;
        let split_index = data.len() - 2 - num_of_elements * 2;
        let mut datas = Vec::new();
        datas.put(&data[..split_index]);
        let mut offsets = Vec::new();
        for i in 0..num_of_elements {
            let start_index = split_index + i * 2;
            offsets.push((&data[start_index..start_index + 2]).get_u16());
        }

        Self {
            data: datas,
            offsets,
        }
    }
}
