pub mod arithmetic;
pub mod cmp;
pub mod boolean;

use crate::DBResult;
use crate::columns::Column;
//use crate::blocks::ColumnBlock;
use crate::types::{TypeName, DBType};

pub trait OpResult
{
    type ResultType:DBType;
    const RESULT_NAME:TypeName = Self::ResultType::NAME;
}

pub trait RegFunction
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>;
    fn to_string(&self, src:Vec<String>) -> String;
}

pub type RegFunctionRef = Box<dyn RegFunction>;

pub trait RegFunctionBuilder
{
    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>;
    fn build(&self, src:Vec<TypeName>) -> DBResult<RegFunctionRef>;
}