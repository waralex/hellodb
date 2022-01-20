pub mod source;
use crate::DBResult;
use crate::columns::Column;
use std::cmp::min;
use cli_table::{Cell, Table, TableStruct, CellStruct, format::{Justify }, Style };
use source::*;


pub struct ColumnBlock
{
    columns:Vec<Column>,
    sources:Vec<ColumnSourceRef>
}

impl ColumnBlock
{
    pub fn new() -> ColumnBlock
    {
        ColumnBlock { columns: Vec::new(), sources: Vec::new() }
    }

    pub fn add(&mut self, column:Column, source:ColumnSourceRef) -> &mut ColumnBlock
    {
        self.columns.push(column);
        self.sources.push(source);
        self
    }

    pub fn is_empty(&self) -> bool
    {
        self.columns.len() == 0
    }

    pub fn cols_len(&self) -> usize
    {
        self.columns.len()
    }
    pub fn rows_len(&self) -> usize
    {
        if self.is_empty()
        {
            0
        }
        else
        {
            self.columns[0].len()
        }
    }

    pub fn col_at(&self, ind:usize) -> &Column
    {
        &self.columns[ind]
    }

    unsafe fn col_at_ptr(&self, ind:usize) -> *const Column
    {
        &self.columns[ind] as *const Column
    }

    pub fn col_at_mut(&mut self, ind:usize) -> &mut Column
    {
        &mut self.columns[ind]
    }

    pub fn process(&mut self, rows:usize) -> DBResult<()>
    {
        for i in 0..self.columns.len()
        {
            self.columns[i].resize(rows);
        }
        for i in 0..self.columns.len()
        {
            self.sources[i].fill_column(&mut self.columns, i)?;
        }
        Ok(())
    }

    //TODO move to separate func or trait
    pub fn cli_table(&self, max_rows:usize) ->TableStruct
    {
        let mut res_vec = Vec::<Vec::<CellStruct>>::new();
        for r in 0..min(max_rows, self.rows_len())
        {
            let mut row_vec = Vec::<CellStruct>::new();
            for c in 0..self.cols_len()
            {
                row_vec.push(
                    self.columns[c].data_ref().
                    to_string_at(r).
                    cell().justify(Justify::Right)
                )
            }
            res_vec.push(row_vec);
        }
        if max_rows < self.rows_len()
        {
            let mut row_vec = Vec::<CellStruct>::new();
            for _ in 0..self.cols_len()
            {
                row_vec.push(
                    "...".
                    cell().justify(Justify::Center)
                )
            }
            res_vec.push(row_vec);
        }
        let mut title_vec = Vec::<CellStruct>::new();
        for c in 0..self.cols_len()
        {
            title_vec.push(
                format!(
                    "{}\n{}",
                    self.columns[c].header().name(),
                    self.columns[c].header().type_name()
                ).
                cell().justify(Justify::Center).bold(true)
            )
        }
        res_vec.table().title(title_vec)
    }

}


#[cfg(test)]
mod test {
    use super::*;
    use crate::columns::header::*;
    use crate::types::*;
    use crate::types::types::*;
    use itertools::izip;
    use crate::functions::regular::arithmetic::*;
    use crate::functions::regular::*;
    #[test]
    fn fun_src()
    {
        let mut block = ColumnBlock::new();
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

        let args = Vec::from([TypeName::DBInt, TypeName::DBInt]);
        block.add(r, DontTouchSource::new_ref());
        block.add(l, DontTouchSource::new_ref());
        let op = PlusBuilder::new().build(args).unwrap();
        block.add(d, FunctionSource::new_ref(Vec::<usize>::from([0, 1]), op));
        block.process(10).unwrap();
        for (i, v) in block.col_at(2).downcast_data_iter::<DBInt>().unwrap().enumerate()
        {
            assert_eq!(*v, (1 + i as i64) * 11);
        }
    }
}