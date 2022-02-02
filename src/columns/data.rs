use std::ops::{DerefMut, Deref};
use crate::types::{DBType, TypeName};
use crate::types::types::*;
use std::any::Any;
use itertools::izip;

#[derive(Debug, Clone)]
pub struct ColumnDataStorage<T:DBType> {
    data: Vec<T::InnerType>
}

impl<T:DBType>  ColumnDataStorage<T> {
    pub fn data_ref(&self) -> &Vec<T::InnerType>
    {
        &self.data
    }
    pub fn data_mut(&mut self) -> &mut Vec<T::InnerType>
    {
        &mut self.data
    }
}

impl<T:DBType> ColumnDataStorage<T> {

    pub fn new() -> ColumnDataStorage<T> {
        ColumnDataStorage { data: Vec::new()}
    }
}

impl<T:DBType> Deref for ColumnDataStorage<T>
{
    type Target = Vec<T::InnerType>;
    fn deref(&self) -> &Self::Target
    {
        &self.data
    }
}

impl<T:DBType> DerefMut for ColumnDataStorage<T>
{
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        &mut self.data
    }
}

pub trait ColumnStorage {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn len(&self) -> usize;
    fn resize(&mut self, size:usize);

    fn fit_offset_limit(&mut self, offset:usize, limit:Option<usize>);

    fn copy_to(&self, dest:&mut Box<dyn ColumnStorage>, offset:usize);
    fn copy_filtered_to(&self, dest:&mut Box<dyn ColumnStorage>, offset:usize, filter:&Box<dyn ColumnStorage>);

    //FIXME: Not the most beautiful, but quickly implemented solution for converting column data to row string data for display
    fn to_string_at(&self, n:usize) -> String;
}

pub type StoragePtr = Box<dyn ColumnStorage>;

impl<T:DBType>  ColumnStorage for ColumnDataStorage<T>
    where T::InnerType:Default
{

    fn as_any(&self) -> &dyn Any
    {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any
    {
        self
    }

    fn len(&self) -> usize
    {
        self.data.len()
    }

    fn resize(&mut self, size:usize)
    {
        self.data.resize(size, Default::default());
    }

    fn fit_offset_limit(&mut self, offset:usize, limit:Option<usize>)
    {
        let size_with_offset = self.len() - offset;
        let res_size = match limit {
            Some(l) => std::cmp::min(l, size_with_offset),
            None => size_with_offset
        };
        if offset > 0
        {
            let dest_ptr = self.data.as_mut_ptr();
            let src_ptr = unsafe {self.data.as_mut_ptr().add(offset)};
            for i in 0..res_size
            {
                unsafe {
                    std::ptr::swap(dest_ptr.add(i), src_ptr.add(i));
                }
            }
        }

        unsafe
        {
            self.data.set_len(res_size);
        }
    }

    fn to_string_at(&self, n:usize) -> String
    {
        self[n].to_string()
    }

    fn copy_to(&self, dest:&mut Box<dyn ColumnStorage>, offset:usize)
    {
        let dest_itr = downcast_storage_mut::<T>(dest).unwrap().iter_mut().skip(offset);
        for (s, d) in izip!(self.data.iter(), dest_itr)
        {
            *d = s.clone();
        }

    }

    fn copy_filtered_to(&self, dest:&mut Box<dyn ColumnStorage>, offset:usize, filter:&Box<dyn ColumnStorage>)
    {
        let mut dest_itr = downcast_storage_mut::<T>(dest).unwrap().iter_mut().skip(offset);
        let mut filter_itr = downcast_storage_ref::<DBInt>(filter).unwrap().iter();
        let mut self_itr = self.data.iter();
        while let Some(flt) = filter_itr.next()
        {
            let src = self_itr.next().unwrap();
            if *flt == 1
            {
                *dest_itr.next().unwrap() = src.clone();
            }
        }
    }

}

impl ColumnStorage for StoragePtr {

    fn as_any(&self) -> &dyn Any
    {
        self.as_ref().as_any()
    }
    fn as_mut_any(&mut self) -> &mut dyn Any
    {
        self.as_mut().as_mut_any()
    }
    fn len(&self) -> usize
    {
        self.as_ref().len()
    }
    fn resize(&mut self, size:usize)
    {
        self.as_mut().resize(size);
    }
    fn fit_offset_limit(&mut self, offset:usize, limit:Option<usize>)
    {
        self.as_mut().fit_offset_limit(offset, limit);
    }
    fn to_string_at(&self, n:usize) -> String
    {
        self.as_ref().to_string_at(n)
    }
    fn copy_to(&self, dest:&mut Box<dyn ColumnStorage>, offset:usize)
    {
        self.as_ref().copy_to(dest, offset)
    }
    fn copy_filtered_to(&self, dest:&mut Box<dyn ColumnStorage>, offset:usize, filter:&Box<dyn ColumnStorage>)
    {
        self.as_ref().copy_filtered_to(dest, offset, filter)
    }


}

pub fn is_storage_of<T:DBType>(col:&dyn ColumnStorage) -> bool
{
    col.as_any().is::<ColumnDataStorage<T>>()
}

pub fn downcast_storage_ref<T:DBType>(col:&dyn ColumnStorage) -> Option<&ColumnDataStorage<T>>
{
    col.as_any().downcast_ref::<ColumnDataStorage<T>>()
}

pub fn downcast_storage_mut<T:DBType>(col:&mut dyn ColumnStorage) -> Option<&mut ColumnDataStorage<T>>
{
    col.as_mut_any().downcast_mut::<ColumnDataStorage<T>>()
}

pub fn make_storage(name:TypeName) -> StoragePtr {
        match name {
            TypeName::DBInt => Box::new(ColumnDataStorage::<DBInt>::new()) as StoragePtr,
            TypeName::DBFloat => Box::new(ColumnDataStorage::<DBFloat>::new()) as StoragePtr,
            TypeName::DBString => Box::new(ColumnDataStorage::<DBString>::new()) as StoragePtr
        }
}

//=============Constant storage============
//TODO: Move to separate mod



#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn unmut_col()
    {
        let c = ColumnDataStorage::<DBInt>::new();
        assert_eq!(c.data, Vec::<i64>::new());
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn mut_col()
    {
        let mut c = ColumnDataStorage::<DBFloat>::new();
        c.push(10.);
        c.push(20.3);
        c.push(30.4);
        assert_eq!(c.data, Vec::<f64>::from([10., 20.3, 30.4]));
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn make_storage_test()
    {
        let c = make_storage(TypeName::DBInt);
        assert!(is_storage_of::<DBInt>(c.as_ref()));

        let c = make_storage(TypeName::DBFloat);
        assert!(!is_storage_of::<DBInt>(c.as_ref()));

        assert!(is_storage_of::<DBFloat>(c.as_ref()));
    }

    #[test]
    fn downcast_storage()
    {
        let mut c = make_storage(TypeName::DBInt);
        let c_mut = downcast_storage_mut::<DBInt>(c.as_mut()).unwrap();
        c_mut.data.push(10);
        c_mut.data.push(20);

        let c_ref = downcast_storage_ref::<DBInt>(c.as_ref()).unwrap();

        assert_eq!(c_ref[0], 10);
        assert_eq!(c_ref[1], 20);
    }

    #[test]
    fn storage_for_storage_ptr()
    {
        let c = make_storage(TypeName::DBInt);
        assert!(is_storage_of::<DBInt>(&c));
        let c_ref = downcast_storage_ref::<DBInt>(&c).unwrap();
        assert_eq!(c_ref.len(), 0);
    }

    #[test]
    fn fit_limit_offset()
    {

        let mut c = ColumnDataStorage::<DBInt>::new();
        let mut test_data = Vec::<i64>::new();
        for i in 1..20
        {
            c.push(i);
            test_data.push(i);
        }
        c.fit_offset_limit(0, None);
        assert_eq!(c.data, test_data);

        c.fit_offset_limit(3, None);
        assert_eq!(c.data, test_data[3..]);

        c.fit_offset_limit(2, Some(10));
        assert_eq!(c.data, test_data[5..15]);

        c.fit_offset_limit(0, Some(3));
        assert_eq!(c.data, test_data[5..8]);

    }

    #[test]
    fn fit_limit_offset_string()
    {

        let mut c = ColumnDataStorage::<DBString>::new();
        let mut test_data = Vec::<String>::new();
        for i in 1..20
        {
            c.push(i.to_string());
            test_data.push(i.to_string());
        }
        c.fit_offset_limit(0, None);
        assert_eq!(c.data, test_data);

        c.fit_offset_limit(3, None);
        assert_eq!(c.data, test_data[3..]);

        c.fit_offset_limit(2, Some(10));
        assert_eq!(c.data, test_data[5..15]);

        c.fit_offset_limit(0, Some(3));
        assert_eq!(c.data, test_data[5..8]);

    }
}