use super::*;
use crate::types::{TypeName, DBType};
use crate::types::types::*;
use std::ops;

/* ======== Any ========== */

pub struct AnyAggrColumn<T:DBType>
{
    data:Vec<T::InnerType>
}

impl<T:DBType> AnyAggrColumn<T>
    where T::InnerType : Clone + Default
{
    pub fn new() -> Self
    {
        Self{data:Vec::new()}
    }
    pub fn new_ref() -> AggrColumnRef
    {
        Box::new(Self::new())
    }
}

impl<T:DBType> AggrColumn for AnyAggrColumn<T>
    where
        T::InnerType : Clone + Default
{

    fn push_empty(&mut self) -> usize
    {
        self.data.push(Default::default());
        self.data.len()
    }
    fn append_value(&mut self, src:Vec<&Column>, src_at:usize, to:usize)
    {
        assert_eq!(src.len(), 1);
        let col = src[0].downcast_data_ref::<T>().unwrap();
        self.data[to] = col[src_at].clone();
    }
    fn finalize_to_column(&mut self, dest:&mut Column)
    {
        let col = dest.downcast_data_mut::<T>().unwrap();
        col.resize(0, Default::default());
        col.append(&mut self.data);
    }
}

pub struct AnyAggrBuilder {}


impl AnyAggrBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn AggrFunctionBuilder>
    {
        Box::new(Self::new())
    }
}

impl AggrFunctionBuilder for AnyAggrBuilder
{

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        if src.len() != 1
        {
            return Err("any expects 1 argument".to_string());
        }
        Ok(src[0])

    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<AggrColumnRef>
    {
        if src.len() != 1
        {
            return Err("any expects 1 argument".to_string());
        }

        match src[0]
        {
            TypeName::DBInt => Ok(AnyAggrColumn::<DBInt>::new_ref()),
            TypeName::DBFloat => Ok(AnyAggrColumn::<DBFloat>::new_ref()),
            TypeName::DBString => Ok(AnyAggrColumn::<DBString>::new_ref()),
        }

    }
}

/* ========== Sum ============ */

pub struct SumAggrColumn<T:DBType>
{
    data:Vec<T::InnerType>
}

impl<T:DBType> SumAggrColumn<T>
    where T::InnerType : ops::AddAssign<
                T::InnerType
                >,
        T::InnerType : Clone + Default
{
    pub fn new() -> Self
    {
        Self{data:Vec::new()}
    }
    pub fn new_ref() -> AggrColumnRef
    {
        Box::new(Self::new())
    }
}

impl<T:DBType> AggrColumn for SumAggrColumn<T>
    where T::InnerType : ops::AddAssign<
                T::InnerType
                >,
        T::InnerType : Clone + Default
{

    fn push_empty(&mut self) -> usize
    {
        self.data.push(Default::default());
        self.data.len()
    }
    fn append_value(&mut self, src:Vec<&Column>, src_at:usize, to:usize)
    {
        assert_eq!(src.len(), 1);
        let col = src[0].downcast_data_ref::<T>().unwrap();
        self.data[to] += col[src_at].clone();
    }
    fn finalize_to_column(&mut self, dest:&mut Column)
    {
        let col = dest.downcast_data_mut::<T>().unwrap();
        col.resize(0, Default::default());
        col.append(&mut self.data);
    }
}

pub struct SumAggrBuilder {}


impl SumAggrBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn AggrFunctionBuilder>
    {
        Box::new(Self::new())
    }
}

impl AggrFunctionBuilder for SumAggrBuilder
{

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        if src.len() != 1
        {
            return Err("sum expects 1 argument".to_string());
        }

        match src[0]
        {
            TypeName::DBInt => Ok(TypeName::DBInt),
            TypeName::DBFloat => Ok(TypeName::DBFloat),
            other => Err(format!("Wrong argument type {} for sum", other))
        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<AggrColumnRef>
    {
        if src.len() != 1
        {
            return Err("sum expects 1 argument".to_string());
        }

        match src[0]
        {
            TypeName::DBInt => Ok(SumAggrColumn::<DBInt>::new_ref()),
            TypeName::DBFloat => Ok(SumAggrColumn::<DBFloat>::new_ref()),
            other => Err(format!("Wrong argument type {} for sum", other))
        }

    }
}