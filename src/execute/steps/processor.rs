use super::*;
use crate::types::types::*;

pub struct ChunkedProcessor<Iter:Iterator<Item = u32>>
{
    sizes_iterator:Iter
}

impl<Iter:Iterator<Item = u32> + 'static> ChunkedProcessor<Iter>
{
    pub fn new(sizes_iterator:Iter) -> Self
    {
        Self{sizes_iterator}
    }

    pub fn new_ref(sizes_iterator:Iter) -> ProcessorRef
    {
        Rc::new(
            RefCell::new(Self::new(sizes_iterator))
        )
    }
}

impl<Iter:Iterator<Item = u32>> Processor for ChunkedProcessor<Iter>
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
}

pub struct FilteredAppendToOutputProcessor
{
    cols_to_copy :Vec<usize>,
    filter_col_index:Option<usize>,
    offset:usize,
    limit:Option<usize>,
    rest_offset:Option<usize>,
    processed:usize,
}

impl FilteredAppendToOutputProcessor
{
    pub fn new(cols_to_copy :Vec<usize>, filter_col_index:Option<usize>, offset:Option<usize>, limit:Option<usize>) -> Self
    {
        Self{cols_to_copy, filter_col_index, offset:offset.unwrap_or(0), limit, processed:0, rest_offset:None}
    }
    pub fn new_ref(cols_to_copy :Vec<usize>, filter_col_index:Option<usize>,
                         offset:Option<usize>, limit:Option<usize>) -> Rc<RefCell<Self>>
    {
        Rc::new(
            RefCell::new(
                Self::new(cols_to_copy, filter_col_index, offset, limit)
            )
        )
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

        if self.processed + add_size as usize <= self.offset
        {
            self.processed += add_size as usize;
            return Ok(ProcessStatus::MustGoOn);
        }

        if self.rest_offset.is_none()
        {
            self.rest_offset = Some(self.offset - self.processed);
        }
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
                if self.processed >= l + self.offset {ProcessStatus::MustStop} else {ProcessStatus::MustGoOn}
            },
            None => ProcessStatus::MustGoOn
        })

    }

}


impl PostProcessor for FilteredAppendToOutputProcessor
{
    fn run(&mut self, output :BlockRef) -> DBResult<()>
    {
        output.borrow_mut().fit_offset_limit(self.rest_offset.unwrap_or(0), self.limit);
        Ok(())
    }
}