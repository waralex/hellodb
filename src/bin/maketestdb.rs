use std::env;
use hellodb::columns::header::ColumnHeader;
use hellodb::io::column::*;
use hellodb::io::db::*;
use hellodb::io::serialize::*;
use hellodb::db::table::*;
use hellodb::types::*;
use hellodb::types::types::*;
use std::path::*;
use rand::prelude::*;
use std::fs::File;
use std::cmp::min;

fn main()
{
    let args: Vec<String> = env::args().collect();
    if args.len() != 2
    {
        println!("Create test database. Usage: maketestdb <path_to_database>");
    }
    let table = Table::new(
        PathBuf::from(&args[1]).join("registrations"),
        "registrations",
        Schema::new(
            Vec::from(
                [
                    ColumnHeader::new("id", TypeName::DBInt),
                    ColumnHeader::new("age", TypeName::DBInt),
                    ColumnHeader::new("platform", TypeName::DBString),
                    ColumnHeader::new("country", TypeName::DBString),
                    ColumnHeader::new("gender", TypeName::DBString),
                    ColumnHeader::new("value", TypeName::DBFloat),
                ]
            )
        )
    );

    make_test_table_dir(PathBuf::from(&args[1]), &table).unwrap();

    let genders = ["Female", "Male"];
    let platforms = ["android", "ios", "web"];
    let countries = ["Russia", "USA", "Belarus", "Ukrine", "Germany", "Italy"];

    let block_size = 2 << 15;

    let mut rows = 1_000_000;

    let mut sizes_file = File::create(table.sizes_file_path()).unwrap();

    let mut id_col = table.make_column("id").unwrap();
    let mut age_col = table.make_column("age").unwrap();
    let mut platform_col = table.make_column("platform").unwrap();
    let mut country_col = table.make_column("country").unwrap();
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
    let mut platform_writer = make_col_writer(
        TypeName::DBString,
        File::create(
            table.col_path("platform").unwrap()
        ).unwrap()
    );
    let mut country_writer = make_col_writer(
        TypeName::DBString,
        File::create(
            table.col_path("country").unwrap()
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
    let mut rng = thread_rng();
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
        for v in age_col.downcast_data_iter_mut::<DBInt>().unwrap()
        {
            *v = rng.gen_range(18..60);
        }
        age_writer.write_col(age_col.data_ref()).unwrap();

        platform_col.resize(bsize);
        for v in platform_col.downcast_data_iter_mut::<DBString>().unwrap()
        {
            *v = platforms[rng.gen_range(0..platforms.len())].to_string();
        }
        platform_writer.write_col(platform_col.data_ref()).unwrap();

        country_col.resize(bsize);
        for v in country_col.downcast_data_iter_mut::<DBString>().unwrap()
        {
            *v = countries[rng.gen_range(0..countries.len())].to_string();
        }
        country_writer.write_col(country_col.data_ref()).unwrap();

        gender_col.resize(bsize);
        for v in gender_col.downcast_data_iter_mut::<DBString>().unwrap()
        {
            *v = genders[rng.gen_range(0..genders.len())].to_string();
        }
        gender_writer.write_col(gender_col.data_ref()).unwrap();

        value_col.resize(bsize);
        for v in value_col.downcast_data_iter_mut::<DBFloat>().unwrap()
        {
            *v = rng.gen_range(0. .. 100.);
        }
        value_writer.write_col(value_col.data_ref()).unwrap();

        println!("{} rows left", rows);
    }

}