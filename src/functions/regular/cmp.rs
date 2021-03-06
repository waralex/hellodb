use super::*;
use crate::types::DBType;
use crate::types::types::*;
use itertools::izip;
use std::cmp::*;
/*============= Equal ===============*/
pub struct Equal<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> Equal<L, R>
{
    pub fn new() -> Self
    {
        Self{
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for Equal<L, R>
{
    type ResultType = DBInt;
}

impl<L:DBType, R:DBType> RegFunction for Equal<L, R>
where L::InnerType:PartialEq<R::InnerType>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<L>().unwrap();
        let r_it = src[1].downcast_data_iter::<R>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if l == r {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct EqualBuilder {}

impl EqualBuilder {
    pub fn new() -> EqualBuilder {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for EqualBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("== operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
           }

        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("== operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(Equal::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(Box::new(Equal::<DBFloat, DBFloat>::new())),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(Box::new(Equal::<DBString, DBString>::new())),
                    _ => Err(err_str)
                }
            }

        }

    }
}

/* ============ NotEqual ============= */

pub struct NotEqual<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> NotEqual<L, R>
{
    pub fn new() ->Self
    {
        Self{
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for NotEqual<L, R>
{
    type ResultType = DBInt;
}

impl<L:DBType, R:DBType> RegFunction for NotEqual<L, R>
where L::InnerType:PartialEq<R::InnerType>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<L>().unwrap();
        let r_it = src[1].downcast_data_iter::<R>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if l != r {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct NotEqualBuilder {}

impl NotEqualBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for NotEqualBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("!= operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
           }

        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("!= operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(NotEqual::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(Box::new(NotEqual::<DBFloat, DBFloat>::new())),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(Box::new(NotEqual::<DBString, DBString>::new())),
                    _ => Err(err_str)
                }
            }

        }

    }
}

/* ======== Less ====== */

pub struct Less<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> Less<L, R>
{
    pub fn new() ->Self
    {
        Self{
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for Less<L, R>
{
    type ResultType = DBInt;
}

impl<L:DBType, R:DBType> RegFunction for Less<L, R>
where L::InnerType:PartialOrd<R::InnerType>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<L>().unwrap();
        let r_it = src[1].downcast_data_iter::<R>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if l < r {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct LessBuilder {}

impl LessBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for LessBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("< operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
           }

        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("< operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(Less::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(Box::new(Less::<DBFloat, DBFloat>::new())),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(Box::new(Less::<DBString, DBString>::new())),
                    _ => Err(err_str)
                }
            }

        }

    }
}

/* ======== LessEqual ====== */

pub struct LessEqual<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> LessEqual<L, R>
{
    pub fn new() ->Self
    {
        Self{
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for LessEqual<L, R>
{
    type ResultType = DBInt;
}

impl<L:DBType, R:DBType> RegFunction for LessEqual<L, R>
where L::InnerType:PartialOrd<R::InnerType>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<L>().unwrap();
        let r_it = src[1].downcast_data_iter::<R>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if l <= r {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct LessEqualBuilder {}

impl LessEqualBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for LessEqualBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("<= operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
           }

        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("< operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(LessEqual::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(Box::new(LessEqual::<DBFloat, DBFloat>::new())),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(Box::new(LessEqual::<DBString, DBString>::new())),
                    _ => Err(err_str)
                }
            }

        }

    }
}

/* ======== Greater ====== */

pub struct Greater<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> Greater<L, R>
{
    pub fn new() ->Self
    {
        Self{
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for Greater<L, R>
{
    type ResultType = DBInt;
}

impl<L:DBType, R:DBType> RegFunction for Greater<L, R>
where L::InnerType:PartialOrd<R::InnerType>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<L>().unwrap();
        let r_it = src[1].downcast_data_iter::<R>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if l > r {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct GreaterBuilder {}

impl GreaterBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for GreaterBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("<= operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
           }

        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("< operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(Greater::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(Box::new(Greater::<DBFloat, DBFloat>::new())),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(Box::new(Greater::<DBString, DBString>::new())),
                    _ => Err(err_str)
                }
            }

        }

    }
}
/* ======== GreaterEqual ====== */

pub struct GreaterEqual<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> GreaterEqual<L, R>
{
    pub fn new() ->Self
    {
        Self{
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for GreaterEqual<L, R>
{
    type ResultType = DBInt;
}

impl<L:DBType, R:DBType> RegFunction for GreaterEqual<L, R>
where L::InnerType:PartialOrd<R::InnerType>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<L>().unwrap();
        let r_it = src[1].downcast_data_iter::<R>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if l >= r {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct GreaterEqualBuilder {}

impl GreaterEqualBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for GreaterEqualBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("<= operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
           }

        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("< operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(GreaterEqual::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(Box::new(GreaterEqual::<DBFloat, DBFloat>::new())),
                    _ => Err(err_str)
                }
            },
            TypeName::DBString => {
                match src[1] {
                    TypeName::DBString => Ok(Box::new(GreaterEqual::<DBString, DBString>::new())),
                    _ => Err(err_str)
                }
            }

        }

    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::columns::header::*;
    use crate::types::*;


    #[test]
    fn plus_builder()
    {
        let args = Vec::from([TypeName::DBInt, TypeName::DBInt]);
        let mut r = Column::new(ColumnHeader::new("r", TypeName::DBInt));
        let mut l = Column::new(ColumnHeader::new("l", TypeName::DBInt));
        let mut d = Column::new(ColumnHeader::new("d", EqualBuilder::new().result_type(args.clone()).unwrap()));
        r.resize(10);
        l.resize(10);
        d.resize(10);

        let r_it = r.downcast_data_iter_mut::<DBInt>().unwrap();
        let l_it = l.downcast_data_iter_mut::<DBInt>().unwrap();
        for (i, r, l) in izip!(0..10, r_it, l_it)
        {
            if i % 2 == 0
            {
                *r = 10;
                *l = 10;
            }
            else
            {
                *r = 1;
                *l = 10;
            }
        }
        let op = EqualBuilder::new().build(args).unwrap();
        op.apply(Vec::from([&r, &l]), &mut d).unwrap();

        for (i, v) in d.downcast_data_iter::<DBInt>().unwrap().enumerate()
        {
            if i % 2 == 0
            {
                assert_eq!(*v, 1);
            }
            else
            {
                assert_eq!(*v, 0);
            }
        }
    }
}