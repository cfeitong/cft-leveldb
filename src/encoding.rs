use bytes::{
    Buf,
    BufMut,
    Bytes,
};

pub trait BufMutExt: BufMut {
    fn put_var_u32_le(&mut self, n: u32);
    fn put_var_u64_le(&mut self, n: u64);
}

const B: u8 = 1 << 7;

impl<T: BufMut> BufMutExt for T {
    fn put_var_u32_le(&mut self, n: u32) {
        match n {
            x if x < (1 << 7) => {
                self.put_u8(n.lowest_u8());
            }
            x if x < (1 << 14) => {
                self.put_u8(n.lowest_u8() | B);
                self.put_u8((n >> 7).lowest_u8())
            }
            x if x < (1 << 21) => {
                self.put_u8(n.lowest_u8() | B);
                self.put_u8((n >> 7).lowest_u8() | B);
                self.put_u8((n >> 14).lowest_u8());
            }
            x if x < (1 << 28) => {
                self.put_u8(n.lowest_u8() | B);
                self.put_u8((n >> 7).lowest_u8() | B);
                self.put_u8((n >> 14).lowest_u8() | B);
                self.put_u8((n >> 21).lowest_u8());
            }
            // x >= (1 << 28)
            _ => {
                self.put_u8(n.lowest_u8() | B);
                self.put_u8((n >> 7).lowest_u8() | B);
                self.put_u8((n >> 14).lowest_u8() | B);
                self.put_u8((n >> 21).lowest_u8() | B);
                self.put_u8((n >> 28).lowest_u8());
            }
        }
    }

    fn put_var_u64_le(&mut self, n: u64) {
        let mut b = n;
        while b >= B as u64 {
            self.put_u8(b.lowest_u8() | B);
            b >>= 7;
        }
        self.put_u8(b.lowest_u8());
    }
}

trait UxExt {
    fn lowest_u8(self) -> u8;
}

impl UxExt for u32 {
    fn lowest_u8(self) -> u8 {
        self.to_le_bytes()[0]
    }
}

impl UxExt for u64 {
    fn lowest_u8(self) -> u8 {
        self.to_le_bytes()[0]
    }
}

pub trait BytesExt {
    fn get_var_u32_le(&mut self) -> Option<u32>;
    fn get_var_u64_le(&mut self) -> Option<u64>;
}

impl BytesExt for Bytes {
    fn get_var_u32_le(&mut self) -> Option<u32> {
        let mut ret = 0;
        let mut base = 1;
        for _ in 0..5 {
            if self.is_empty() {
                return None;
            }
            let cur = self.get_u8();
            let is_end = cur & B == 0;
            let cur = (cur & !B) as u32;
            ret += cur * base;
            base <<= 7;
            if is_end {
                break;
            }
        }
        Some(ret)
    }

    fn get_var_u64_le(&mut self) -> Option<u64> {
        let mut ret = 0;
        let mut base = 1;
        for _ in 0..10 {
            if self.is_empty() {
                return None;
            }
            let cur = self.get_u8();
            let is_end = cur & B == 0;
            let cur = (cur & !B) as u64;
            ret += cur * base;
            base <<= 7;
            if is_end {
                break;
            }
        }
        Some(ret)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_var_u32_le() {
        let mut buf = BytesMut::new();
        for i in 0u32..32 * 32 {
            let v = (i / 32) << (i % 32);
            buf.put_var_u32_le(v);
        }
        let mut buf = buf.freeze();
        for i in 0u32..32 * 32 {
            let v = (i / 32) << (i % 32);
            let t = buf.get_var_u32_le();
            assert_eq!(Some(v), t);
        }
        assert!(buf.get_var_u32_le().is_none());
    }

    #[test]
    fn test_var_u64_le() {
        // special cases
        let mut data: Vec<u64> = vec![0, 100, !0, !0 - 1];
        for i in 0..64 {
            let base = 1 << i;
            data.push(base);
            data.push(base - 1);
            data.push(base + 1);
        }
        let mut buf = BytesMut::new();
        for &v in &data {
            buf.put_var_u64_le(v);
        }

        let mut buf = buf.freeze();
        for &v in &data {
            assert_eq!(Some(v), buf.get_var_u64_le());
        }
        assert!(buf.get_var_u64_le().is_none());
    }
}
