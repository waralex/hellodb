use std::io::{Read, Write};
pub trait NativeByte {}


macro_rules! make_native_byte {
    ($inner_ty:ty) => {
        impl NativeByte for $inner_ty {
        }
    };
}

make_native_byte!(i8);
make_native_byte!(i16);
make_native_byte!(i32);
make_native_byte!(i64);
make_native_byte!(u8);
make_native_byte!(u16);
make_native_byte!(u32);
make_native_byte!(u64);
make_native_byte!(f32);
make_native_byte!(f64);

pub trait ByteSerialize
{
    fn to_byte(&self, dest:&mut impl Write) -> std::io::Result<()>;
    fn from_byte(&mut self, src: &mut (impl Read + ?Sized)) -> std::io::Result<()>;
    fn size_in_bytes(&self) -> usize;
}

impl<T:NativeByte + Default> ByteSerialize for Vec<T>
{
    fn size_in_bytes(&self) -> usize
    {
        self.len() * std::mem::size_of::<T>()
    }
    fn to_byte(&self, dest:&mut impl Write) -> std::io::Result<()>
    {
        let ptr = self.as_ptr() as *const u8;
        let  slice = unsafe {
            std::slice::from_raw_parts(ptr, self.size_in_bytes())
        };
        dest.write_all(&slice)?;
        dest.flush()?;
        Ok(())
    }
    fn from_byte(&mut self, src: &mut (impl Read + ?Sized)) -> std::io::Result<()>
    {
        let ptr = self.as_mut_ptr() as *mut u8;
        let  slice = unsafe {
            std::slice::from_raw_parts_mut(ptr, self.size_in_bytes())
        };
        src.read_exact(slice)
    }
}

impl<T:NativeByte> ByteSerialize for T
{
    fn size_in_bytes(&self) -> usize
    {
        std::mem::size_of::<T>()
    }
    fn to_byte(&self, dest:&mut impl Write) -> std::io::Result<()>
    {
        let ptr = self as *const T as  *const u8;
        let  slice = unsafe {
            std::slice::from_raw_parts(ptr, self.size_in_bytes())
        };
        dest.write_all(&slice)?;
        dest.flush()?;
        Ok(())
    }
    fn from_byte(&mut self, src: &mut (impl Read + ?Sized)) -> std::io::Result<()>
    {
        let ptr = self as *mut T as  *mut u8;
        let  slice = unsafe {
            std::slice::from_raw_parts_mut(ptr, self.size_in_bytes())
        };
        src.read_exact(slice)
    }
}

/*
FIXME: This is quite inefficient due to the large number of allocations in the process.
The best solution would be to write a type that stores an array of strings in a flat form,
but this is beyond the scope of this prototype
*/
impl ByteSerialize for Vec<String>
{
    fn size_in_bytes(&self) -> usize
    {
        let total_len = self.iter().fold(0, |len, itm| len + itm.len());
        self.len() * std::mem::size_of::<u32>() + total_len
    }
    fn to_byte(&self, dest:&mut impl Write) -> std::io::Result<()>
    {
        let lengths:Vec<u32> = self.iter().map(|itm| itm.len() as u32).collect();
        lengths.to_byte(dest)?;
        for s in self.iter()
        {
            dest.write_all(s.as_bytes())?;
        }
        dest.flush()?;
        Ok(())
    }
    fn from_byte(&mut self, src: &mut (impl Read + ?Sized)) -> std::io::Result<()>
    {
        let mut lengths = Vec::<u32>::new();
        lengths.resize(self.len(), 0);
        lengths.from_byte(src)?;
        let mut byte_buffer = Vec::<u8>::new();
        for (l, s) in lengths.into_iter().zip(self.iter_mut())
        {
            byte_buffer.resize(l as usize, 0);
            byte_buffer.from_byte(src)?;
            *s = String::from_utf8_lossy(&byte_buffer).into_owned();
        }
        Ok(())
    }
}
impl ByteSerialize for String
{
    fn size_in_bytes(&self) -> usize
    {
        std::mem::size_of::<u32>() + self.len()
    }
    fn to_byte(&self, dest:&mut impl Write) -> std::io::Result<()>
    {
        (self.len() as u32).to_byte(dest)?;
        dest.write_all(self.as_bytes())?;
        Ok(())
    }
    fn from_byte(&mut self, src: &mut (impl Read + ?Sized)) -> std::io::Result<()>
    {
        let mut length:u32 = 0;
        length.from_byte(src)?;
        let mut byte_buffer = Vec::<u8>::new();
        byte_buffer.resize(length as usize, 0);
        byte_buffer.from_byte(src)?;
        *self = String::from_utf8_lossy(&byte_buffer).into_owned();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn vec_io_i8()
    {
        let v = Vec::<i8>::from([1,2,3,4,5]);
        let mut buff = Vec::<u8>::with_capacity(5);
        v.to_byte(&mut buff).unwrap();
        assert_eq!(buff, Vec::<u8>::from([1,2,3,4,5]));
        let mut r = Vec::<i8>::new();
        r.resize(v.len(), 0);
        r.from_byte(&mut buff.as_slice()).unwrap();
        assert_eq!(v, r);
    }
    #[test]
    fn vec_io_i16()
    {
        let v = Vec::<i16>::from([1,2,3,4,5]);
        let mut buff = Vec::<u8>::with_capacity(5);
        v.to_byte(&mut buff).unwrap();
        let mut r = Vec::<i16>::new();
        r.resize(v.len(), 0);
        r.from_byte(&mut buff.as_slice()).unwrap();
        assert_eq!(v, r);
    }
    #[test]
    fn single_io()
    {
        let v:i16 = 258;
        let mut buff = Vec::<u8>::with_capacity(2);
        v.to_byte(&mut buff).unwrap();
        let mut r:i16 = 0;
        r.from_byte(&mut buff.as_slice()).unwrap();
        assert_eq!(v, r);

    }
    #[test]
    fn string_vec_io()
    {
        let v = Vec::<String>::from(["1".to_string(), "231".to_string(), "a".to_string(), "bbbbb".to_string()]);
        let mut buff = Vec::<u8>::with_capacity(20);
        v.to_byte(&mut buff).unwrap();
        let mut r = Vec::<String>::new();
        r.resize(v.len(), Default::default());
        r.from_byte(&mut buff.as_slice()).unwrap();
        assert_eq!(v, r);
    }
    #[test]
    fn string_io()
    {
        let v = "Test Пи".to_string();
        let mut buff = Vec::<u8>::with_capacity(20);
        v.to_byte(&mut buff).unwrap();
        let mut r = String::new();
        r.from_byte(&mut buff.as_slice()).unwrap();
        assert_eq!(v, r);
    }
}