use super::*;
use std::io::Read;
use crate::io::column::{ColReaderPtr, make_col_reader};
use crate::types::{TypeName, DBType};
use crate::functions::regular::RegFunctionRef;
use crate::columns::Column;
pub trait ColumnSource
{
    fn fill_column(&mut self, block:&mut Vec<Column>, col_ind:usize) -> DBResult<()>;
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
    fn fill_column(&mut self, _block:&mut Vec<Column>, _col_ind:usize) -> DBResult<()>
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
    fn fill_column(&mut self, block:&mut Vec<Column>, col_ind:usize) -> DBResult<()>
    {
        match self.reader.read_col(block[col_ind].data_mut()) {
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
    fn fill_column(&mut self, block:&mut Vec<Column>, col_ind:usize) -> DBResult<()>
    {
        for v in block[col_ind].downcast_data_iter_mut::<T>().unwrap() {
            *v = self.value.clone()
        }

        Ok(())
    }
}


pub struct FunctionSource
{
    args_inds :Vec<usize>,
    func: RegFunctionRef
}

impl FunctionSource
{
    pub fn new(args_inds:Vec<usize>, func:RegFunctionRef) -> FunctionSource
    {
        FunctionSource{args_inds, func}
    }
    pub fn new_ref(args_inds:Vec<usize>, func:RegFunctionRef) -> ColumnSourceRef
    {
        Box::new(FunctionSource::new(args_inds, func))
    }
}

impl ColumnSource for FunctionSource
{
    fn fill_column(&mut self, block:&mut Vec<Column>, col_ind:usize) -> DBResult<()>
    {
        let mut args = Vec::<&Column>::new();
        assert!(self.args_inds.iter().find(|&&i| i == col_ind).is_none());
        //If the condition above is met, this block is safe
        unsafe{
            for i in self.args_inds.iter()
            {
                let ptr = &block[*i] as *const Column;
                args.push(&*ptr);
            }
        }
        self.func.apply(args, &mut block[col_ind])
    }
}