use super::*;
use crate::types::types::*;
use crate::io::db::BlockSizeIter;

pub struct ChunkedProcessor
{
    sizes_iterator:BlockSizeIter
}

impl ChunkedProcessor
{
    pub fn new(sizes_iterator:BlockSizeIter) -> Self
    {
        Self{sizes_iterator}
    }
}

impl Processor for ChunkedProcessor
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

pub struct FilteredAppendToOutputProcessor
{
    cols_to_copy :Vec<usize>,
    filter_col_index:Option<usize>,
    limit:Option<usize>,
    processed:usize
}

impl FilteredAppendToOutputProcessor
{
    pub fn new(cols_to_copy :Vec<usize>, filter_col_index:Option<usize>, limit:Option<usize>) -> Self
    {
        Self{cols_to_copy, filter_col_index, limit, processed:0}
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
        self.processed += add_size as usize;

        Ok(match self.limit {
            Some(l) => {
                if self.processed >= l {ProcessStatus::MustStop} else {ProcessStatus::MustGoOn}
            },
            None => ProcessStatus::MustGoOn
        })
    }

    fn finalize(&mut self, output :BlockRef) -> DBResult<()>
    {
        match self.limit {
            Some(l) => {
                let sz = std::cmp::min(l, output.borrow().rows_len());
                output.borrow_mut().resize(sz);
            },
            None => {}
        };
        Ok(())
    }

}