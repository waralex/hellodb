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
    pub fn new() ->Equal<L, R>
    {
        Equal{
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
            },
            _ => Err(err_str)

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
            },
            _ => Err(err_str)

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