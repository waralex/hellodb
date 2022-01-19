mod serialize;
use std::io::{Read, Write};

use crate::columns::Column;
use crate::columns::data::*;
use serialize::ByteSerialize;
use lz4::{Decoder, EncoderBuilder};
use lz4_sys::{LZ4_compressBound};

pub struct ChunkWriter<T, W:Write> where Vec<T>:ByteSerialize {
    dest: W,
    compressed_buff :Vec<u8>,
    _marker: std::marker::PhantomData<T>
}

impl<T, W:Write> ChunkWriter<T, W> where Vec<T>:ByteSerialize {
    pub fn new(dest:W) -> ChunkWriter<T, W>
    {
        ChunkWriter{
            dest,
            compressed_buff : Vec::new(),
            _marker : std::marker::PhantomData::<T>{}
        }
    }
    pub fn write(&mut self, data:&Vec<T>) -> std::io::Result<()>
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

    pub fn dest(&self) -> &W
    {
        &self.dest
    }
}

pub struct ChunkReader<T:Default+Clone, R:Read> where Vec<T>:ByteSerialize {
    src: R,
    compressed_buff : Vec<u8>,
    _marker: std::marker::PhantomData<T>
}

impl<T:Default+Clone, R:Read> ChunkReader<T, R> where Vec<T>:ByteSerialize {
    pub fn new(src:R) -> ChunkReader<T, R>
    {
        ChunkReader{
            src,
            compressed_buff : Vec::new(),
            _marker : std::marker::PhantomData::<T>{}
        }
    }
    pub fn read(&mut self, data:&mut Vec<T>) -> std::io::Result<()>
    {

        let mut chunk_size:u32 = 0;
        let mut uncompressed_size:u32 = 0;
        let mut compressed_size:u32 = 0;
        chunk_size.from_byte(&mut self.src)?;
        uncompressed_size.from_byte(&mut self.src)?;
        compressed_size.from_byte(&mut self.src)?;

        self.compressed_buff.resize(compressed_size as usize, 0);
        self.src.read_exact(self.compressed_buff.as_mut_slice())?;

        let mut decoder = Decoder::new(self.compressed_buff.as_slice())?;
        data.resize(chunk_size as usize, Default::default());
        data.from_byte(&mut decoder)?;
        Ok(())

    }

}


#[cfg(test)]
mod test
{
    use super::*;
    //use crate::columns::Column;
    #[test]
    fn write_read_chunk()
    {

        let dest = Vec::<u8>::new();
        let mut writer = ChunkWriter::<i16, Vec<u8>>::new(dest);
        let data = Vec::<i16>::from([1,2,10, 258]);
        writer.write(&data).unwrap();
        assert!(writer.dest().len() > 4*3);
        assert!(writer.dest().len() < 4*3 + 4*16);

        let mut reader = ChunkReader::<i16, &[u8]>::new(writer.dest().as_slice());
        let mut res = Vec::<i16>::new();
        reader.read(&mut res).unwrap();
        assert_eq!(res.len(), data.len());
        assert_eq!(res, data);

    }

    #[test]
    fn write_read_chunk_strings()
    {

        let dest = Vec::<u8>::new();
        let mut writer = ChunkWriter::<String, Vec<u8>>::new(dest);
        let data = Vec::<String>::from(["1".to_string(), "231".to_string(), "a".to_string(), "bbbbb".to_string()]);
        writer.write(&data).unwrap();

        let mut reader = ChunkReader::<String, &[u8]>::new(writer.dest().as_slice());
        let mut res = Vec::<String>::new();
        reader.read(&mut res).unwrap();
        assert_eq!(res.len(), data.len());
        assert_eq!(res, data);
    }

}