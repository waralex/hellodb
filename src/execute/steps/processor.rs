use super::*;
use crate::types::types::*;
struct ChunkedProcessor<T:Iterator<Item = i32>>
{
    sizes_iterator:T
}

impl<T:Iterator<Item = i32>> ChunkedProcessor<T>
{
    pub fn new(sizes_iterator:T) -> Self
    {
        Self{sizes_iterator}
    }
}

impl<T:Iterator<Item = i32>> Processor for ChunkedProcessor<T>
{

    fn run(&mut self, input :BlockRef, _output :BlockRef) -> DBResult<ProcessStatus>
    {
        if let Some(size) = self.sizes_iterator.next()
        {
            input.borrow_mut().process(size as usize)?;
            Ok(ProcessStatus::MustGoOn)
        }
        else {
            Ok(ProcessStatus::MustStop)
        }
    }

    fn finalize(&mut self, _output :BlockRef) -> DBResult<()>
    {
        Ok(())
    }

}

struct FilteredAppendToOutputProcessor
{
    cols_to_copy :Vec<usize>,
    filter_col_index:Option<usize>
}

impl FilteredAppendToOutputProcessor
{
    pub fn new(cols_to_copy :Vec<usize>, filter_col_index:Option<usize>) -> Self
    {
        Self{cols_to_copy, filter_col_index}
    }
}

impl Processor for FilteredAppendToOutputProcessor
{

    fn run(&mut self, input :BlockRef, output :BlockRef) -> DBResult<ProcessStatus>
    {
        let input = input.borrow();
        let filter_col = match self.filter_col_index {
           Some(i) => Some(input.col_at(i)),
           None => None
        };
        let add_size:i64 = match filter_col {
            Some(col) => col.downcast_data_iter::<DBInt>().unwrap().sum(),
            None => input.rows_len() as i64
        };
        let mut out = output.borrow_mut();
        let offset = out.rows_len();
        out.resize(offset + add_size as usize);
        for (out_i, in_i) in self.cols_to_copy.iter().enumerate()
        {
            match filter_col {
                Some(col) => input.col_at(*in_i).copy_filtered_to(out.col_at_mut(out_i), offset, col),
                None => input.col_at(*in_i).copy_to(out.col_at_mut(out_i), offset)
            };
        }

        Ok(ProcessStatus::MustGoOn)
    }

    fn finalize(&mut self, _output :BlockRef) -> DBResult<()>
    {
        Ok(())
    }

}