use std::ops::{DerefMut, Deref};
use crate::types::{DBType, TypeName};
use crate::types::types::*;
use std::any::Any;

#[derive(Debug, Clone)]
pub struct ColumnDataStorage<T:DBType> {
    data: Vec<T::InnerType>
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
}

pub type StoragePtr = Box<dyn ColumnStorage>;

impl<T:DBType>  ColumnStorage for ColumnDataStorage<T> {

    fn as_any(&self) -> &dyn Any
    {
        self
    }
    fn as_mut_any(&mut self) -> &mut dyn Any
    {
        self
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
            TypeName::Int8 => Box::new(ColumnDataStorage::<Int8>::new()) as StoragePtr,
            TypeName::Int16 => Box::new(ColumnDataStorage::<Int16>::new()) as StoragePtr
        }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn unmut_col()
    {
        let c = ColumnDataStorage::<Int8>::new();
        assert_eq!(c.data, Vec::<i8>::new());
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn mut_col()
    {
        let mut c = ColumnDataStorage::<Int16>::new();
        c.push(10);
        c.push(20);
        c.push(30);
        assert_eq!(c.data, Vec::<i16>::from([10, 20, 30]));
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn make_storage_test()
    {
        let c = make_storage(TypeName::Int8);
        assert!(is_storage_of::<Int8>(c.as_ref()));

        let c = make_storage(TypeName::Int16);
        assert!(!is_storage_of::<Int8>(c.as_ref()));

        assert!(is_storage_of::<Int16>(c.as_ref()));
    }

    #[test]
    fn downcast_storage()
    {
        let mut c = make_storage(TypeName::Int8);
        let c_mut = downcast_storage_mut::<Int8>(c.as_mut()).unwrap();
        c_mut.data.push(10);
        c_mut.data.push(20);

        let c_ref = downcast_storage_ref::<Int8>(c.as_ref()).unwrap();

        assert_eq!(c_ref[0], 10);
        assert_eq!(c_ref[1], 20);
    }

    #[test]
    fn storage_for_storage_ptr()
    {
        let c = make_storage(TypeName::Int8);
        assert!(is_storage_of::<Int8>(&c));
        let c_ref = downcast_storage_ref::<Int8>(&c).unwrap();
        assert_eq!(c_ref.len(), 0);
    }
}