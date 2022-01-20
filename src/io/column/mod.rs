mod native;


use crate::types::{DBType, TypeName};
use crate::types::types::*;
use super::serialize::ByteSerialize;
use native::{ChunkWriter, ChunkReader};
use std::io::{Read, Write};
use crate::columns::data::{StoragePtr};

pub trait ColDataWriter {
    fn write_col(&mut self, col_data:&StoragePtr) -> std::io::Result<()>;
}

impl<T:DBType, W:Write> ColDataWriter for ChunkWriter<T, W> where Vec<T::InnerType>:ByteSerialize {

    fn write_col(&mut self, col_data:&StoragePtr) -> std::io::Result<()>
    {
        self.write_col_data(col_data)
    }

}

pub type ColWriterPtr = Box<dyn ColDataWriter>;

//FIXME:Replace with a macro when I become more familiar with it
pub fn make_col_writer<W:Write + 'static>(name:TypeName, dest:W) -> ColWriterPtr {
        match name {
            TypeName::DBInt => Box::new(
                        ChunkWriter::<DBInt, W>::new(dest)
                    ) as ColWriterPtr,
            TypeName::DBFloat => Box::new(
                    ChunkWriter::<DBFloat, W>::new(dest)
                ) as ColWriterPtr,
            TypeName::DBString => Box::new(
                    ChunkWriter::<DBString, W>::new(dest)
            ) as ColWriterPtr,
        }
}

pub trait ColDataReader {
    fn read_col(&mut self, col_data:&mut StoragePtr) -> std::io::Result<()>;
}

impl<T:DBType, R:Read> ColDataReader for ChunkReader<T, R>
    where Vec<T::InnerType>:ByteSerialize,
        T::InnerType:Default+Clone
{
    fn read_col(&mut self, col_data:&mut StoragePtr) -> std::io::Result<()>
    {
        self.read_col_data(col_data)
    }
}

pub type ColReaderPtr = Box<dyn ColDataReader>;

pub fn make_col_reader<R:Read + 'static>(name:TypeName, src:R) -> ColReaderPtr {
        match name {
            TypeName::DBInt => Box::new(
                        ChunkReader::<DBInt, R>::new(src)
                    ) as ColReaderPtr,
            TypeName::DBFloat => Box::new(
                    ChunkReader::<DBFloat, R>::new(src)
                ) as ColReaderPtr,
            TypeName::DBString => Box::new(
                    ChunkReader::<DBString, R>::new(src)
            ) as ColReaderPtr,
        }
}




#[cfg(test)]
mod test {
    use super::*;
    use crate::columns::data::*;
    use std::fs::File;
    use std::path::Path;
    fn cleanup_file(p:&str)
    {
        let path = Path::new(p);
        if path.exists()
        {
            std::fs::remove_file(path).unwrap();
        }
    }
    #[test]
    fn write_read()
    {
        cleanup_file("./test.col");
        let file = File::create("./test.col").unwrap();
        let mut writer = make_col_writer(TypeName::DBInt, file);
        let mut c = make_storage(TypeName::DBInt);
        let c_mut = downcast_storage_mut::<DBInt>(c.as_mut()).unwrap();
        c_mut.data_mut().push(10);
        c_mut.data_mut().push(20);
        c_mut.data_mut().push(30);
        c_mut.data_mut().push(40);
        writer.write_col(&c).unwrap();

        let file = File::open("./test.col").unwrap();

        let mut reader = make_col_reader(TypeName::DBInt, file);
        let mut res = make_storage(TypeName::DBInt);
        reader.read_col(&mut res).unwrap();
        let res_ref = downcast_storage_ref::<DBInt>(res.as_ref()).unwrap();
        let c_ref = downcast_storage_ref::<DBInt>(c.as_ref()).unwrap();

        assert_eq!(res_ref.data_ref().len(), c_ref.data_ref().len());
        assert_eq!(*res_ref.data_ref(), *c_ref.data_ref());

        cleanup_file("./test.col");
    }
}