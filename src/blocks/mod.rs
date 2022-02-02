pub mod source;
use crate::DBResult;
use crate::columns::Column;
use std::collections::HashMap;
use std::cmp::min;
use cli_table::{Cell, Table, TableStruct, CellStruct, format::{Justify }, Style };
use std::rc::Rc;
use std::cell::RefCell;
use source::*;


pub struct ColumnBlock
{
    columns:Vec<Column>,
    sources:Vec<ColumnSourceRef>,
    name_index:HashMap<String, usize>
}

pub type BlockRef = Rc<RefCell<ColumnBlock>>;

impl ColumnBlock
{
    pub fn new() -> ColumnBlock
    {
        ColumnBlock { columns: Vec::new(), sources: Vec::new(), name_index: HashMap::new() }
    }

    pub fn add(&mut self, column:Column, source:ColumnSourceRef) -> &mut ColumnBlock
    {
        self.name_index.insert(column.name().to_string(), self.columns.len());
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

    pub fn has_col(&self, name:&str) -> bool
    {
        self.name_index.contains_key(name)
    }
    pub fn col_index_by_name(&self, name:&str) -> Option<usize>
    {
        match self.name_index.get(name) {
            Some(v) => Some(v.clone()),
            None => None
        }
    }

    pub fn col_at(&self, ind:usize) -> &Column
    {
        &self.columns[ind]
    }

    pub fn col_at_mut(&mut self, ind:usize) -> &mut Column
    {
        &mut self.columns[ind]
    }
    pub fn resize(&mut self, rows:usize)
    {
        for i in 0..self.columns.len()
        {
            self.columns[i].resize(rows);
        }
    }
    pub fn fit_offset_limit(&mut self, offset:usize, limit:Option<usize>)
    {
        for i in 0..self.columns.len()
        {
            self.columns[i].fit_offset_limit(offset, limit);
        }
    }

    pub fn process(&mut self, rows:usize) -> DBResult<()>
    {
        self.resize(rows);
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
    use crate::columns::data::*;
    use crate::types::*;
    use crate::types::types::*;
    use itertools::izip;
    use crate::functions::regular::arithmetic::*;
    use crate::functions::regular::cmp::*;
    use crate::functions::regular::*;
    use std::fs::File;
    use std::path::{Path, PathBuf};
    use crate::io::column::*;
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
    #[test]
    fn srcs()
    {
        let test_path = PathBuf::from("block_tst");
        if test_path.exists()
        {
            std::fs::remove_dir_all(&test_path).unwrap();
        }
        std::fs::create_dir(&test_path).unwrap();

        let a_path = test_path.join("a.col");
        let b_path = test_path.join("b.col");
        let c_path = test_path.join("c.col");

        let a_file = File::create(&a_path).unwrap();
        let b_file = File::create(&b_path).unwrap();
        let c_file = File::create(&c_path).unwrap();
        let mut a_writer = make_col_writer(TypeName::DBInt, a_file);
        let mut b_writer = make_col_writer(TypeName::DBInt, b_file);
        let mut c_writer = make_col_writer(TypeName::DBInt, c_file);
        let mut a_strg = make_storage(TypeName::DBInt);
        let mut b_strg = make_storage(TypeName::DBInt);
        let mut c_strg = make_storage(TypeName::DBInt);

        a_strg.resize(4);
        b_strg.resize(4);
        c_strg.resize(4);

        let a_data = downcast_storage_mut::<DBInt>(a_strg.as_mut()).unwrap().data_mut();
        a_data.copy_from_slice(&([1, 2, 3, 4] as [i64;4]));
        a_writer.write_col(&a_strg).unwrap();

        let b_data = downcast_storage_mut::<DBInt>(b_strg.as_mut()).unwrap().data_mut();
        b_data.copy_from_slice(&([10, 20, 30, 40] as [i64;4]));
        b_writer.write_col(&b_strg).unwrap();

        let c_data = downcast_storage_mut::<DBInt>(c_strg.as_mut()).unwrap().data_mut();
        c_data.copy_from_slice(&([11, 6, 33, 8] as [i64;4]));
        c_writer.write_col(&c_strg).unwrap();

        let a_data = downcast_storage_mut::<DBInt>(a_strg.as_mut()).unwrap().data_mut();
        a_data.copy_from_slice(&([5, 6, 7, 8] as [i64;4]));
        a_writer.write_col(&a_strg).unwrap();

        let b_data = downcast_storage_mut::<DBInt>(b_strg.as_mut()).unwrap().data_mut();
        b_data.copy_from_slice(&([50, 60, 70, 80] as [i64;4]));
        b_writer.write_col(&b_strg).unwrap();

        let c_data = downcast_storage_mut::<DBInt>(c_strg.as_mut()).unwrap().data_mut();
        c_data.copy_from_slice(&([11, 66, 33, 88] as [i64;4]));
        c_writer.write_col(&c_strg).unwrap();

        let a_file = File::open(&a_path).unwrap();
        let b_file = File::open(&b_path).unwrap();
        let c_file = File::open(&c_path).unwrap();

        let mut block = ColumnBlock::new();
        let args = Vec::from([TypeName::DBInt, TypeName::DBInt]);

        block.add(
            Column::new(ColumnHeader::new("a", TypeName::DBInt)),
            ExternalSource::new_ref(a_file, TypeName::DBInt),
        ).add(
            Column::new(ColumnHeader::new("b", TypeName::DBInt)),
            ExternalSource::new_ref(b_file, TypeName::DBInt),
        ).add(
            Column::new(ColumnHeader::new("c", TypeName::DBInt)),
            ExternalSource::new_ref(c_file, TypeName::DBInt),
        ).add(
            Column::new(ColumnHeader::new("a + b", TypeName::DBInt)),
            FunctionSource::new_ref(
                vec![0, 1], PlusBuilder::new().build(args.clone()).unwrap()
            )
        ).add(
            Column::new(ColumnHeader::new("a + b == c", TypeName::DBInt)),
            FunctionSource::new_ref(
                vec![2, 3], EqualBuilder::new().build(args.clone()).unwrap()
            )

        );


        block.process(4).unwrap();
        let res:Vec<i64> = block.col_at(4).downcast_data_iter::<DBInt>().unwrap().copied().collect();
        assert_eq!(res, vec![1, 0, 1, 0]);
        block.process(4).unwrap();
        let res:Vec<i64> = block.col_at(4).downcast_data_iter::<DBInt>().unwrap().copied().collect();
        assert_eq!(res, vec![0, 1, 0, 1]);

        if test_path.exists()
        {
            std::fs::remove_dir_all(&test_path).unwrap();
        }
    }
}