use super::*;
use std::io::Read;
use crate::io::column::{ColReaderPtr, make_col_reader};
use crate::types::{TypeName, DBType};
use crate::functions::regular::RegFunctionRef;
use crate::columns::Column;
pub trait ColumnSource
{
    fn fill_column(&mut self, columns:&mut HashMap<String,Column>, col_name:&str) -> DBResult<()>;
}

pub type ColumnSourceRef = Box<dyn ColumnSource>;

pub struct DontTouchSource
{
}
impl DontTouchSource
{
    pub fn new() -> DontTouchSource {DontTouchSource{}}
    pub fn new_ref() -> ColumnSourceRef
    {
        Box::new(DontTouchSource::new())
    }
}

impl ColumnSource for DontTouchSource
{
    fn fill_column(&mut self, _columns:&mut HashMap<String, Column>, _col_name:&str) -> DBResult<()>
    {
        Ok(())
    }
}

pub struct ExternalSource
{
    reader :ColReaderPtr,
}

impl ExternalSource
{
    pub fn new<R:Read + 'static>(src:R, name:TypeName) -> ExternalSource
    {
        ExternalSource{reader:make_col_reader::<R>(name, src)}
    }
    pub fn new_ref<R:Read + 'static>(src:R, name:TypeName) -> ColumnSourceRef
    {
        Box::new(ExternalSource::new(src, name))
    }
}

impl ColumnSource for ExternalSource
{
    fn fill_column(&mut self, columns:&mut HashMap<String, Column>, col_name:&str) -> DBResult<()>
    {
        match self.reader.read_col(columns.get_mut(col_name).unwrap().data_mut()) {
            Ok(_) => Ok(()),
            Err(r) => Err(r.to_string())
        }
    }
}

pub struct ConstValueSource<T:DBType>
{
    value :T::InnerType
}

impl<T:DBType> ConstValueSource<T>
{
    pub fn new(value:T::InnerType) -> Self
    {
        Self{value:value.clone()}
    }
    pub fn new_ref(value:T::InnerType) -> ColumnSourceRef
    {
        Box::new(Self::new(value))
    }
}

impl<T:DBType> ColumnSource for ConstValueSource<T>
{
    fn fill_column(&mut self, columns:&mut HashMap<String, Column>, col_name:&str) -> DBResult<()>
    {
        columns.get_mut(col_name).unwrap().downcast_data_mut::<T>().unwrap().fill(self.value.clone());

        Ok(())
    }
}


pub struct FunctionSource
{
    args :Vec<String>,
    func: RegFunctionRef
}

impl FunctionSource
{
    pub fn new(args:Vec<String>, func:RegFunctionRef) -> FunctionSource
    {
        FunctionSource{args, func}
    }
    pub fn new_ref(args:Vec<String>, func:RegFunctionRef) -> ColumnSourceRef
    {
        Box::new(FunctionSource::new(args, func))
    }
}

impl ColumnSource for FunctionSource
{
    fn fill_column(&mut self, columns:&mut HashMap<String, Column>, col_name:&str) -> DBResult<()>
    {
        let mut args = Vec::<&Column>::new();
        //If the condition above is met, this block is safe
        unsafe{
            for i in self.args.iter()
            {
                let ptr = &columns[i] as *const Column;
                args.push(&*ptr);
            }
        }
        self.func.apply(args, columns.get_mut(col_name).unwrap())
    }
}