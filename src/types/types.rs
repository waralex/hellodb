use super::{DBType, TypeName};

macro_rules! make_type {
    ($name:ident, $inner_ty:ty, $data_ty:expr) => {
        #[derive(Debug, Clone)]
        pub struct $name {}

        impl DBType for $name {
            type InnerType = $inner_ty;
            const NAME: TypeName = $data_ty;
            fn to_type_string(&self) -> &str
            {
                stringify!($name)
            }
        }
    };
}

make_type!(Int8, i8, TypeName::Int8);
make_type!(Int16, i16, TypeName::Int16);


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn base_types()
    {
        let t = Int8{};
        assert_eq!(t.to_type_string(), "Int8");
        assert_eq!(Int8::NAME, TypeName::Int8);
    }
}