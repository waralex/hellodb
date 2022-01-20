use std::io::{Read, Write};

use crate::types::DBType;
use crate::columns::data::*;
use crate::io::serialize::ByteSerialize;
use lz4::{Decoder, EncoderBuilder};
use lz4_sys::{LZ4_compressBound};

pub struct ChunkWriter<T:DBType, W:Write> where Vec<T::InnerType>:ByteSerialize {
    dest: W,
    compressed_buff :Vec<u8>,
    _marker: std::marker::PhantomData<T>
}

impl<T:DBType, W:Write> ChunkWriter<T, W> where Vec<T::InnerType>:ByteSerialize {
    pub fn new(dest:W) -> ChunkWriter<T, W>
    {
        ChunkWriter{
            dest,
            compressed_buff : Vec::new(),
            _marker : std::marker::PhantomData::<T>{}
        }
    }
    pub fn write(&mut self, data:&Vec<T::InnerType>) -> std::io::Result<()>
    {
        let compress_bound: usize = unsafe { LZ4_compressBound(data.size_in_bytes() as i32) as usize};
        self.compressed_buff.resize(compress_bound, 0);
        //FIXME: Since the encoder becomes the owner of the writer
        //then we have to create an instance of  the encoder for each chunk
        let mut encoder = EncoderBuilder::new()
        .level(2)
        .build(self.compressed_buff.as_mut_slice())?;

        data.to_byte(&mut encoder)?;

        let chunk_size:u32 = data.len() as u32;
        let uncompressed_size:u32 = data.size_in_bytes() as u32;
        let compressed_size:u32 = (compress_bound - encoder.writer().len()) as u32;
        chunk_size.to_byte(&mut self.dest)?;
        uncompressed_size.to_byte(&mut self.dest)?;
        compressed_size.to_byte(&mut self.dest)?;
        self.dest.write(&self.compressed_buff[..(compressed_size as usize)])?;
        Ok(())

    }

    #[cfg(test)]
    pub fn dest(&self) -> &W
    {
        &self.dest
    }

    pub fn write_col_data(&mut self, col_ptr:&StoragePtr) -> std::io::Result<()>
    {
        let col = downcast_storage_ref::<T>(col_ptr).unwrap();
        self.write(col.data_ref())
    }
}

pub struct ChunkReader<T:DBType, R:Read>
    where Vec<T::InnerType>:ByteSerialize,
            T::InnerType:Default+Clone
{
    src: R,
    compressed_buff : Vec<u8>,
    _marker: std::marker::PhantomData<T>
}

impl<T:DBType, R:Read> ChunkReader<T, R>
    where Vec<T::InnerType>:ByteSerialize,
        T::InnerType:Default+Clone
    {
    pub fn new(src:R) -> ChunkReader<T, R>
    {
        ChunkReader{
            src,
            compressed_buff : Vec::new(),
            _marker : std::marker::PhantomData::<T>{}
        }
    }
    pub fn read(&mut self, data:&mut Vec<T::InnerType>) -> std::io::Result<()>
    {

        let mut chunk_size:u32 = 0;
        let mut uncompressed_size:u32 = 0;
        let mut compressed_size:u32 = 0;
        chunk_size.from_byte(&mut self.src)?;
        uncompressed_size.from_byte(&mut self.src)?;
        compressed_size.from_byte(&mut self.src)?;
        if chunk_size != data.len() as u32
        {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "chunk don't match dest data size"))
        }
        self.compressed_buff.resize(compressed_size as usize, 0);
        self.src.read_exact(self.compressed_buff.as_mut_slice())?;

        let mut decoder = Decoder::new(self.compressed_buff.as_slice())?;
        data.from_byte(&mut decoder)?;
        Ok(())

    }

    pub fn read_col_data(&mut self, col_ptr:&mut StoragePtr) -> std::io::Result<()>
    {
        let col = downcast_storage_mut::<T>(col_ptr).unwrap();
        self.read(col.data_mut())
    }

}


#[cfg(test)]
mod test
{
    use super::*;
    use crate::types::TypeName;
    use crate::types::types::*;
    #[test]
    fn write_read_chunk()
    {

        let dest = Vec::<u8>::new();
        let mut writer = ChunkWriter::<DBInt, Vec<u8>>::new(dest);
        let data = Vec::<i64>::from([1,2,10, 258]);
        writer.write(&data).unwrap();
        assert!(writer.dest().len() > 4*3);
        assert!(writer.dest().len() < 4*3 + 4*16);

        let mut reader = ChunkReader::<DBInt, &[u8]>::new(writer.dest().as_slice());
        let mut res = Vec::<i64>::new();
        res.resize(data.len(), Default::default());
        reader.read(&mut res).unwrap();
        assert_eq!(res.len(), data.len());
        assert_eq!(res, data);

    }

    #[test]
    fn write_read_chunk_strings()
    {

        let dest = Vec::<u8>::new();
        let mut writer = ChunkWriter::<DBString, Vec<u8>>::new(dest);
        let data = Vec::<String>::from(["1".to_string(), "231".to_string(), "a".to_string(), "bbbbb".to_string()]);
        writer.write(&data).unwrap();

        let mut reader = ChunkReader::<DBString, &[u8]>::new(writer.dest().as_slice());
        let mut res = Vec::<String>::new();
        res.resize(data.len(), Default::default());
        reader.read(&mut res).unwrap();
        assert_eq!(res.len(), data.len());
        assert_eq!(res, data);
    }

    #[test]
    fn write_read_col_storage()
    {

        let mut c = make_storage(TypeName::DBInt);
        let c_mut = downcast_storage_mut::<DBInt>(c.as_mut()).unwrap();
        c_mut.data_mut().push(10);
        c_mut.data_mut().push(20);
        c_mut.data_mut().push(30);
        c_mut.data_mut().push(40);

        let dest = Vec::<u8>::new();
        let mut writer = ChunkWriter::<DBInt, Vec<u8>>::new(dest);
        writer.write_col_data(&c).unwrap();
        assert!(writer.dest().len() > 4*3);
        assert!(writer.dest().len() < 4*3 + 4*16);

        let mut reader = ChunkReader::<DBInt, &[u8]>::new(writer.dest().as_slice());
        let mut res = make_storage(TypeName::DBInt);
        res.resize(c.len());
        reader.read_col_data(&mut res).unwrap();

        let res_ref = downcast_storage_ref::<DBInt>(res.as_ref()).unwrap();
        let c_ref = downcast_storage_ref::<DBInt>(c.as_ref()).unwrap();

        assert_eq!(res_ref.data_ref().len(), c_ref.data_ref().len());
        assert_eq!(*res_ref.data_ref(), *c_ref.data_ref());

    }

}