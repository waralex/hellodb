use super::*;
use crate::types::DBType;
use crate::types::types::*;
use itertools::izip;
use std::ops;
/*========== Plus ==================*/
pub struct Plus<L, R>
{
    _l :std::marker::PhantomData<L>,
    _r :std::marker::PhantomData<R>,
}

impl<L, R> Plus<L, R>
{
    pub fn new() -> Plus<L, R>
    {
        Plus{
            _l:std::marker::PhantomData::<L>{},
            _r:std::marker::PhantomData::<R>{},
        }
    }
}

impl OpResult for Plus<DBInt, DBInt>
{
    type ResultType = DBInt;
}
impl OpResult for Plus<DBFloat, DBFloat>
{
    type ResultType = DBFloat;
}

impl<L:DBType, R:DBType> RegFunction for Plus<L, R>
where L::InnerType:ops::Add<
            R::InnerType,
            Output = <<Plus<L, R> as OpResult>::ResultType as DBType>::InnerType
        >,
      Plus<L, R>:OpResult,
      L::InnerType:Copy,
      R::InnerType:Copy,
{
    fn apply(&self, src:Vec<&Column>, dest:&mut Column) -> DBResult<()>
    {
        assert_eq!(src.len(), 2);

        let l_it = src[0].downcast_data_iter::<L>().unwrap();
        let r_it = src[1].downcast_data_iter::<R>().unwrap();

        let dest_it = dest.downcast_data_iter_mut::<<Self as OpResult>::ResultType>().unwrap();

        for (l, r, d) in izip!(l_it, r_it, dest_it)
        {
            *d = *l + *r;
        }

        Ok(())
    }
    fn to_string(&self, src:Vec<String>) -> String
    {
        assert_eq!(src.len(), 2);
        format!("{} + {}", src[0], src[1])
    }
}

pub struct PlusBuilder {}

impl PlusBuilder {
    pub fn new() -> PlusBuilder {Self{}}
}
impl RegFunctionBuilder for PlusBuilder {

    fn result_type(&self, src:Vec<TypeName>) -> DBResult<TypeName>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("+ operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(TypeName::DBInt),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(TypeName::DBFloat),
                    _ => Err(err_str)
                }
            },
            _ => Err(err_str)

        }
    }
    fn build(&self, src:Vec<TypeName>) -> DBResult<Box<dyn RegFunction>>
    {
        assert_eq!(src.len(), 2);
        let err_str = format!("+ operation unsupported for {} and {}", src[0], src[1]);
        match src[0] {
            TypeName::DBInt => {
                match src[1] {
                    TypeName::DBInt => Ok(Box::new(Plus::<DBInt, DBInt>::new())),
                    _ => Err(err_str)
                }
            }
            TypeName::DBFloat => {
                match src[1] {
                    TypeName::DBFloat => Ok(Box::new(Plus::<DBFloat, DBFloat>::new())),
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
    use crate::types::types::*;

    #[test]
    fn op_result() {
        assert_eq!(Plus::<DBInt, DBInt>::RESULT_NAME, TypeName::DBInt);
        assert_eq!(Plus::<DBFloat, DBFloat>::RESULT_NAME, TypeName::DBFloat);
    }

    #[test]
    fn plus_impl()
    {
        let mut r = Column::new(ColumnHeader::new("r", TypeName::DBInt));
        let mut l = Column::new(ColumnHeader::new("l", TypeName::DBInt));
        let mut d = Column::new(ColumnHeader::new("d", TypeName::DBInt));
        r.resize(10);
        l.resize(10);
        d.resize(10);

        let r_it = r.downcast_data_iter_mut::<DBInt>().unwrap();
        let l_it = l.downcast_data_iter_mut::<DBInt>().unwrap();
        for (i, r, l) in izip!(0..10, r_it, l_it)
        {
            *r = (i as i64 + 1) * 10;
            *l = i as i64 + 1;
        }
        let op = Plus::<DBInt, DBInt>::new();
        op.apply(Vec::from([&r, &l]), &mut d).unwrap();

        for (i, v) in d.downcast_data_iter::<DBInt>().unwrap().enumerate()
        {
            assert_eq!(*v, (1 + i as i64) * 11);
        }
    }

    #[test]
    fn plus_res_type()
    {
        assert_eq!(
            PlusBuilder::new().result_type(
                Vec::from([TypeName::DBInt, TypeName::DBInt])
            ), Ok(TypeName::DBInt)
        );
        assert_eq!(
            PlusBuilder::new().result_type(
                Vec::from([TypeName::DBFloat, TypeName::DBFloat])
            ), Ok(TypeName::DBFloat)
        );
        assert!(
            PlusBuilder::new().result_type(
                Vec::from([TypeName::DBInt, TypeName::DBFloat])
            ).is_err()

        );

    }
    #[test]
    fn plus_builder()
    {
        let args = Vec::from([TypeName::DBInt, TypeName::DBInt]);
        let mut r = Column::new(ColumnHeader::new("r", TypeName::DBInt));
        let mut l = Column::new(ColumnHeader::new("l", TypeName::DBInt));
        let mut d = Column::new(ColumnHeader::new("d", PlusBuilder::new().result_type(args.clone()).unwrap()));
        r.resize(10);
        l.resize(10);
        d.resize(10);

        let r_it = r.downcast_data_iter_mut::<DBInt>().unwrap();
        let l_it = l.downcast_data_iter_mut::<DBInt>().unwrap();
        for (i, r, l) in izip!(0..10, r_it, l_it)
        {
            *r = (i as i64 + 1) * 10;
            *l = i as i64 + 1;
        }
        let op = PlusBuilder::new().build(args).unwrap();
        op.apply(Vec::from([&r, &l]), &mut d).unwrap();

        for (i, v) in d.downcast_data_iter::<DBInt>().unwrap().enumerate()
        {
            assert_eq!(*v, (1 + i as i64) * 11);
        }
    }
}