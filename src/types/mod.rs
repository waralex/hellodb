pub mod types;
use std::fmt::{Debug, Display};
use std::fmt;



#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TypeName
{
    DBInt,
    DBFloat,
    DBString
}

impl TryFrom<String> for TypeName {
    type Error = &'static str;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.as_ref() {
            "Int" => Ok(TypeName::DBInt),
            "Float" => Ok(TypeName::DBFloat),
            "String" => Ok(TypeName::DBString),
            _ => Err("undefined type")
        }
    }
}
impl Display for TypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
             "{}",
             match self {
                 TypeName::DBInt => "Int",
                 TypeName::DBFloat => "Float",
                 TypeName::DBString => "String",
             }
        )
    }
}

pub trait DBType: Clone + Debug + 'static{
    type InnerType:Clone + Debug + ToString;
    const NAME:TypeName;
    const STR_NAME:&'static str;
    fn to_type_string(&self) -> &str;
}



#[cfg(test)]
mod test
{
    use super::*;
    #[test]
    fn to_string()
    {
        assert_eq!(TypeName::DBFloat.to_string(), "Float")
    }

}