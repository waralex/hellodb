use super::{DBType, TypeName};

macro_rules! make_type {
    ($name:ident, $display_name:expr, $inner_ty:ty) => {
        #[derive(Debug, Clone)]
        pub struct $name {}

        impl DBType for $name {
            type InnerType = $inner_ty;
            const NAME: TypeName = TypeName::$name;
            const STR_NAME:&'static str = $display_name;
            fn to_type_string(&self) -> &str
            {
                $display_name
            }
        }
    };
}

make_type!(DBInt, "Int", i64);
make_type!(DBFloat, "Float", f64);
make_type!(DBString, "String", String);


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn base_types()
    {
        let t = DBInt{};
        assert_eq!(t.to_type_string(), "Int");
        assert_eq!(DBInt::NAME, TypeName::DBInt);
    }
}