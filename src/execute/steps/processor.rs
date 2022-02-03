use super::*;
use crate::types::types::*;
use std::cmp::Ordering;

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
    filter_col_name:Option<String>,
    offset:usize,
    limit:Option<usize>,
    rest_offset:Option<usize>,
    processed:usize,
}

impl FilteredAppendToOutputProcessor
{
    pub fn new(filter_col_name:Option<String>, offset:Option<usize>, limit:Option<usize>) -> Self
    {
        Self{filter_col_name, offset:offset.unwrap_or(0), limit, processed:0, rest_offset:None}
    }
    pub fn new_ref(filter_col_name:Option<String>,
                         offset:Option<usize>, limit:Option<usize>) -> Rc<RefCell<Self>>
    {
        Rc::new(
            RefCell::new(
                Self::new(filter_col_name, offset, limit)
            )
        )
    }

}

impl Processor for FilteredAppendToOutputProcessor
{

    fn run(&mut self, input :BlockRef, output :BlockRef) -> DBResult<ProcessStatus>
    {
        if self.limit.is_some()
        {
            if self.processed >= self.limit.unwrap() + self.offset
            {
                return Ok(ProcessStatus::MustStop);
            }
        }

        let input = input.borrow();
        let filter_col = match &self.filter_col_name {
           Some(v) => Some(input.col_at(v)),
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

        for (name, out_col) in out.col_iter_mut()
        {
            match filter_col {
                Some(fcol) => input.col_at(name).copy_filtered_to(out_col, offset, fcol),
                None => input.col_at(name).copy_to(out_col, offset)
            };

        }
        self.processed += add_size as usize;

        Ok(ProcessStatus::MustGoOn)

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

pub struct OrderByPostProcessor
{
    fields:Vec<(String, bool)>,
    offset:usize,
    limit:Option<usize>
}

impl OrderByPostProcessor
{
    pub fn new(fields:Vec<(String, bool)>, offset:Option<usize>, limit:Option<usize>) -> Self
    {
        Self{fields, offset:offset.unwrap_or(0), limit}
    }
    pub fn new_ref(fields:Vec<(String, bool)>, offset:Option<usize>, limit:Option<usize>) -> Rc<RefCell<Self>>
    {
        Rc::new(
            RefCell::new(
                Self::new(fields, offset, limit)
            )
        )
    }

    fn cmp_func(&self, a:usize, b:usize, block:&ColumnBlock) -> Ordering
    {
        for (fld, asc) in self.fields.iter()
        {
            let col = block.col_at(fld);
            let cmp_res = if *asc {
                col.elems_cmp(a, b)
            } else {
                col.elems_cmp(b, a)
            };
            match cmp_res
            {
                Ordering::Equal => continue,
                other => return other
            }
        }
        Ordering::Equal
    }

}


impl PostProcessor for OrderByPostProcessor
{
    fn run(&mut self, output_ref :BlockRef) -> DBResult<()>
    {
        let mut output = output_ref.borrow_mut();
        let mut perms:Vec<usize> = (0..output.rows_len()).collect();

        perms.sort_unstable_by(
            |a, b| self.cmp_func(*a, *b, &output)
        );

        let to = match self.limit {
            Some(l) => std::cmp::min(l + self.offset, perms.len()),
            None => perms.len()
        };

        output.permute(&perms[self.offset..to]);
        Ok(())
    }
}