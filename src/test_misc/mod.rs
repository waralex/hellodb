use crate::columns::header::ColumnHeader;
use crate::io::column::*;
use crate::io::db::*;
use crate::io::serialize::*;
use crate::db::table::*;
use crate::types::*;
use crate::types::types::*;
use std::path::*;
use rand::prelude::*;
use std::fs::File;
use std::cmp::min;
use crate::db::DB;

pub fn create_test_db(path:impl AsRef<Path>, size:usize) -> DB
{

    let table = Table::new(
        path.as_ref().join("regs"),
        "regs",
        Schema::new(
            Vec::from(
                [
                    ColumnHeader::new("id", TypeName::DBInt),
                    ColumnHeader::new("age", TypeName::DBInt),
                    ColumnHeader::new("gender", TypeName::DBString),
                    ColumnHeader::new("value", TypeName::DBFloat),
                ]
            )
        )
    );

    make_test_table_dir(path.as_ref(), &table).unwrap();

    let genders = ["Female", "Male"];

    let block_size = 6;

    let mut rows = size;

    let mut sizes_file = File::create(table.sizes_file_path()).unwrap();

    let mut id_col = table.make_column("id").unwrap();
    let mut age_col = table.make_column("age").unwrap();
    let mut gender_col = table.make_column("gender").unwrap();
    let mut value_col = table.make_column("value").unwrap();

    let mut id_writer = make_col_writer(
        TypeName::DBInt,
        File::create(
            table.col_path("id").unwrap()
        ).unwrap());
    let mut age_writer = make_col_writer(
        TypeName::DBInt,
        File::create(
            table.col_path("age").unwrap()
        ).unwrap()
    );
    let mut gender_writer = make_col_writer(
        TypeName::DBString,
        File::create(table.col_path("gender").unwrap()
        ).unwrap()
    );
    let mut value_writer = make_col_writer(
        TypeName::DBFloat,
        File::create(
            table.col_path("value").unwrap()
        ).unwrap()
    );

    let mut id:i64 = 1;
    let mut age:i64 = size as i64 + 1;
    let mut gender_p = 1;
    while rows > 0
    {
        let bsize = min(block_size, rows);
        rows -= bsize;
        (bsize as u32).to_byte(&mut sizes_file).unwrap();
        id_col.resize(bsize);
        for v in id_col.downcast_data_iter_mut::<DBInt>().unwrap()
        {
            *v = id;
            id += 1;
        }
        id_writer.write_col(id_col.data_ref()).unwrap();

        age_col.resize(bsize);
        for (i, v) in age_col.downcast_data_iter_mut::<DBInt>().unwrap().enumerate()
        {
            *v = age;
            age -= 1;
        }
        age_writer.write_col(age_col.data_ref()).unwrap();

        gender_col.resize(bsize);
        for v in gender_col.downcast_data_iter_mut::<DBString>().unwrap()
        {
            *v = genders[(gender_p % 2) as usize].to_string();
            gender_p += 1;
        }
        gender_writer.write_col(gender_col.data_ref()).unwrap();

        value_col.resize(bsize);
        for (i, v) in value_col.downcast_data_iter_mut::<DBFloat>().unwrap().enumerate()
        {
            *v = i as f64 / 2.;
        }
        value_writer.write_col(value_col.data_ref()).unwrap();
    }
    DB::open(path.as_ref()).unwrap()
}

pub fn cleanup_test_table(path:impl AsRef<Path>)
{
    if path.as_ref().exists()
    {
        std::fs::remove_dir_all(path.as_ref()).unwrap();
    }
}