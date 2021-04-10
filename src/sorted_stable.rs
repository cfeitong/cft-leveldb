use bytes::{
    BufMut,
    Bytes,
    BytesMut,
};

use crate::encoding::BufMutExt;

const RESTART_THRESHOLD: usize = 16;

struct BlockHandle {}

struct BlockBuilder {
    buf:      BytesMut,
    restarts: Vec<usize>,
    count:    usize,
    last_key: Bytes,
}

impl BlockBuilder {
    fn new() -> Self {
        BlockBuilder {
            buf:      BytesMut::new(),
            restarts: vec![],
            count:    0,
            last_key: Bytes::new(),
        }
    }

    fn add(&mut self, key: Bytes, value: Bytes) {
        let mut shared = 0;
        if self.count < RESTART_THRESHOLD {
            shared = shared_prefix_len(&self.last_key, &key);
        } else {
            self.restarts.push(self.buf.len());
            self.count = 0;
        }
        let non_shared = key.len() - shared;
        self.buf.put_var_u32_le(shared as u32);
        self.buf.put_var_u32_le(non_shared as u32);
        self.buf.put_var_u32_le(value.len() as u32);
        self.buf.extend(key.slice(shared..));
        self.buf.extend(value);
        self.last_key = key;
        self.count += 1;
    }

    fn build(mut self) -> Bytes {
        let len = self.restarts.len();
        for i in self.restarts {
            self.buf.put_u32_le(i as u32);
        }
        self.buf.put_u32_le(len as u32);
        self.buf.freeze()
    }
}

fn shared_prefix_len(key1: &Bytes, key2: &Bytes) -> usize {
    key1.iter()
        .zip(key2.iter())
        .take_while(|(&a, &b)| a == b)
        .count()
}
