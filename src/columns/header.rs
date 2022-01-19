use crate::types::TypeName;
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnHeader
{
    name :String,
    type_name : TypeName,
}

impl ColumnHeader {
    pub fn new(name:&str, type_name : TypeName) -> ColumnHeader
    {
        ColumnHeader{name:String::from(name), type_name}
    }
    pub fn name(&self) -> &str
    {
        &self.name
    }
    pub fn type_name(&self) -> TypeName
    {
        self.type_name
    }
}

