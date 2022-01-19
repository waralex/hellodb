pub mod types;
use enum_iterator::IntoEnumIterator;
use std::fmt::Debug;



#[derive(Clone, Copy, Debug, PartialEq, IntoEnumIterator)]
pub enum TypeName
{
    Int8,
    Int16
}

pub trait DBType: Clone + Debug + 'static{
    type InnerType:Clone + Debug;
    const NAME:TypeName;
    fn to_type_string(&self) -> &str;
}




#[cfg(test)]
mod test
{
    //use super::*;

}