pub mod header;
pub mod data;

use header::ColumnHeader;
use crate::types::{DBType, TypeName};
use data::*;


pub struct Column
{

    header :ColumnHeader,
    data :StoragePtr
}

impl Column {

    pub fn new(header:ColumnHeader) -> Self
    {
        let type_name = header.type_name();
        Self{
            header,
            data : make_storage(type_name)
        }
    }

    pub fn header(&self) -> &ColumnHeader
    {
        &self.header
    }

    pub fn name(&self) -> &str
    {
        &self.header.name()
    }
    pub fn type_name(&self) -> TypeName
    {
        self.header.type_name()
    }

    pub fn data_ref(&self) -> &StoragePtr
    {
        &self.data
    }
    pub fn data_mut(&mut self) -> &mut StoragePtr
    {
        &mut self.data
    }

    pub fn clone_empty(&self) -> Self
    {
        Self::new(self.header.clone())
    }

    pub fn len(&self) -> usize
    {
        self.data.len()
    }

    pub fn resize(&mut self, size:usize)
    {
        self.data.resize(size);
    }
    pub fn fit_offset_limit(&mut self, offset:usize, limit:Option<usize>)
    {
        self.data.fit_offset_limit(offset, limit);
    }
    pub fn copy_to(&self, dest:&mut Column, offset:usize)
    {
        self.data.copy_to(dest.data_mut(), offset);
    }
    pub fn copy_filtered_to(&self, dest:&mut Column, offset:usize, filter:&Column)
    {
        self.data.copy_filtered_to(dest.data_mut(), offset, filter.data_ref());
    }

    pub fn downcast_data_ref<T:DBType>(&self) -> Option<&ColumnDataStorage<T>>
    {
        downcast_storage_ref::<T>(&self.data)
    }
    pub fn downcast_data_mut<T:DBType>(&mut self) -> Option<&mut ColumnDataStorage<T>>
    {
        downcast_storage_mut::<T>(&mut self.data)
    }

    pub fn downcast_data_iter<T:DBType>(&self) -> Option<impl Iterator<Item = &T::InnerType>>
    {
        match self.downcast_data_ref::<T>() {
            Some(d) => Some(d.iter()),
            None => None
        }
    }
    pub fn downcast_data_iter_mut<T:DBType>(&mut self) -> Option<impl Iterator<Item = &mut T::InnerType>>
    {
        match self.downcast_data_mut::<T>() {
            Some(d) => Some(d.iter_mut()),
            None => None
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::types::TypeName;
    use crate::types::types::*;
    use crate::columns::data::is_storage_of;

    #[test]
    fn new_column()
    {
        let mut c = Column::new(ColumnHeader::new("test", TypeName::DBInt));
        assert!(is_storage_of::<DBInt>(c.data_ref()));
        let ci8 = c.downcast_data_mut::<DBInt>().unwrap();
        ci8.push(10);
        ci8.push(20);
        let ci8 = c.downcast_data_ref::<DBInt>().unwrap();
        assert_eq!(ci8[0], 10);
        assert_eq!(ci8[1], 20);
    }

    #[test]
    fn data_iter()
    {
        let mut c = Column::new(ColumnHeader::new("test", TypeName::DBInt));
        let ci8 = c.downcast_data_mut::<DBInt>().unwrap();
        ci8.push(10);
        ci8.push(20);

        let res:Vec<i64> = c.downcast_data_iter::<DBInt>().unwrap().copied().collect();
        assert_eq!(res, Vec::<i64>::from([10, 20]));
    }
    #[test]
    fn data_mut_iter()
    {
        let mut c = Column::new(ColumnHeader::new("test", TypeName::DBInt));
        c.resize(10);
        for (i, v) in c.downcast_data_iter_mut::<DBInt>().unwrap().enumerate()
        {
            *v = i as i64;
        }


        for (i, &v) in c.downcast_data_iter::<DBInt>().unwrap().enumerate()
        {
            assert_eq!(i as i64, v);
        }

    }
}
