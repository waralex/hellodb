use super::*;
use crate::types::types::*;
use itertools::izip;
/*============= And ===============*/
pub struct And<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> And<L, R>
{
    pub fn new() -> Self
    {
        Self {
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for And<L, R>
{
    type ResultType = DBInt;
}

impl RegFunction for And<DBInt, DBInt>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<DBInt>().unwrap();
        let r_it = src[1].downcast_data_iter::<DBInt>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if (*l == 1) && (*r == 1) {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct AndBuilder {}

impl AndBuilder {
    pub fn new() -> AndBuilder {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for AndBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("and operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            _ => Err(err_str)
        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("and operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(And::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            },
            _ => Err(err_str)
        }

    }
}

/*============= Or ===============*/
pub struct Or<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> Or<L, R>
{
    pub fn new() -> Self
    {
        Self {
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl<L, R> OpResult for Or<L, R>
{
    type ResultType = DBInt;
}

impl RegFunction for Or<DBInt, DBInt>
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<DBInt>().unwrap();
        let r_it = src[1].downcast_data_iter::<DBInt>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = if (*l == 1) || (*r == 1) {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} == {}", src[0], src[1])
    }
}

pub struct OrBuilder {}

impl OrBuilder {
    pub fn new() -> OrBuilder {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for OrBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("or operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            _ => Err(err_str)
        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("and operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(Or::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            },
            _ => Err(err_str)
        }

    }
}

/*============= Or ===============*/
pub struct Not
{
}

impl Not
{
    pub fn new() -> Self
    {
        Self {
        }
    }
}

impl OpResult for Not
{
    type ResultType = DBInt;
}

impl RegFunction for Not
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 1);

        let it = src[0].downcast_data_iter::<DBInt>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<DBInt>().unwrap();

        for (s, d) in izip!(it, dest_it)
        {
            *d = if *s == 0  {1} else {0};
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 1);
        format!("!{}", src[0])
    }
}

pub struct NotBuilder {}

impl NotBuilder {
    pub fn new() -> Self {Self{}}
    pub fn new_ref() -> Box<dyn RegFunctionBuilder>
    {
        Box::new(Self::new())
    }
}
impl RegFunctionBuilder for NotBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 1);
        let err_str = format!("not operation unsupported for {}", src[0]);
        match src[0] {
            TypeName::DBInt => {
                match src[0] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            },
            _ => Err(err_str)
        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 1);
        let err_str = format!("not operation unsupported for {}", src[0]);
        match src[0] {
            TypeName::DBInt =>  Ok(Box::new(Not::new())),
            _ => Err(err_str)
        }

    }
}
