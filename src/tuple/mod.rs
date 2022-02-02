use crate::types::{TypeName, DBType};

pub struct DBTuple
{
    types :Vec<TypeName>
}

impl DBTuple
{
    pub fn new(types: Vec<TypeName>) -> Self
    {
        Self{types}
    }


}

