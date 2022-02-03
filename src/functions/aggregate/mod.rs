pub mod simple;

use crate::DBResult;
use crate::columns::Column;
use crate::types::TypeName;
use simple::*;

pub trait AggrColumn
{
    fn push_empty(&mut self) -> usize;
    fn append_value(&mut self, src:Vec<&Column>, src_at:usize, to:usize);
    fn finalize_to_column(&mut self, dest:&mut Column);
}

pub type AggrColumnRef = Box<dyn AggrColumn>;

pub trait AggrFunctionBuilder
{
    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>;
    fn build(&self, src:Vec<TypeName>) -> DBResult<AggrColumnRef>;
}

pub type AggrFunctionBuilderRef = Box<dyn AggrFunctionBuilder>;

pub fn is_aggregate_function(name:&str) -> bool
{
    match name.to_lowercase().as_ref() {
        "sum" | "any"  => true,
        _ => false
    }
}

pub fn aggregate_function_builder(name:&str) -> Option<AggrFunctionBuilderRef>
{
    match name.to_lowercase().as_ref() {
        "sum"  => Some(SumAggrBuilder::new_ref()),
        "any"  => Some(AnyAggrBuilder::new_ref()),
        _ => None
    }
}