/*use hellodb::columns::*;
use hellodb::io::column::*;
use std::any::Any;

struct tst_i<T> {
    data :Vec<T>
}

trait is_tst:Any {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn len(&self) -> usize;
}

impl<T:'static> is_tst for tst_i<T>{
    fn as_mut_any(&mut self) -> &mut dyn Any
    {
        self
    }
    fn as_any(&self) -> &dyn Any
    {
        self
    }
    fn len(&self) -> usize
    {
        self.data.len()
    }
}

struct tst {
    b :Box<dyn is_tst>
}
fn ps(v:&mut Box<dyn is_tst>)
{
    let t = (*v).as_any().downcast_ref::<tst_i<i32>>().unwrap();
    t.data.push(12);
}
*/
fn main() {
}