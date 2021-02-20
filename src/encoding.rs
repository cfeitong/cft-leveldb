use bytes::BufMut;

pub trait BufMutExt: BufMut {
    fn put_var_u32_le(&mut self, n: u32);
    fn put_var_u64_le(&mut self, n: u64);
}

impl<T: BufMut> BufMutExt for T {
    fn put_var_u32_le(&mut self, n: u32) {
        unimplemented!()
    }

    fn put_var_u64_le(&mut self, n: u64) {
        unimplemented!()
    }
}
