use super::serialize::ByteSerialize;
use crate::db::table::{Schema, Table};
use crate::db::DB;
use crate::columns::header::ColumnHeader;
use crate::types::TypeName;
use std::path::*;
use std::fs::*;

pub fn read_schema(p: impl AsRef<Path>) -> std::io::Result<Schema> {
    let mut f = File::open(p)?;
    let mut cols_n:u32 = 0;
    cols_n.from_byte(&mut f)?;
    let mut headers = Vec::<ColumnHeader>::new();
    for _ in 0..cols_n
    {
        let mut name = String::new();
        let mut type_string = String::new();
        name.from_byte(&mut f)?;
        type_string.from_byte(&mut f)?;
        headers.push(
            ColumnHeader::new(
                &name,
                TypeName::try_from(type_string).unwrap()
            )
        )
    }
    Ok(Schema::from(headers))
}

pub fn write_schema(p:impl AsRef<Path>, s:&Schema) -> std::io::Result<()>
{
    let mut f = File::create(p)?;
    (s.len() as u32).to_byte(&mut f)?;
    for h in s.headers_ref().iter()
    {
        h.name().to_string().to_byte(&mut f)?;
        h.type_name().to_string().to_byte(&mut f)?;
    }
    Ok(())

}

pub fn make_test_table_dir(db_path:impl AsRef<Path>, table:&Table) -> std::io::Result<()>
{
    let table_path = db_path.as_ref().join(PathBuf::from(table.name()));
    create_dir_all(&table_path)?;
    write_schema(&table_path.join("schema.bin"), table.schema())?;
    Ok(())
}

pub fn open_table(db_path:impl AsRef<Path>, name:impl AsRef<str>) -> std::io::Result<Table>
{
    let table_path = db_path.as_ref().join(PathBuf::from(name.as_ref()));
    let schema = read_schema(table_path.join("schema.bin"))?;
    Ok(Table::new(table_path, name.as_ref(), schema))
}

pub fn make_test_database(db_path:impl AsRef<Path>, tables:&[Table]) -> std::io::Result<()>
{
    for t in tables
    {
        make_test_table_dir(&db_path, t)?;
    }
    Ok(())
}

pub fn open_database(db_path:impl AsRef<Path>) -> std::io::Result<DB>
{
    let mut tables = Vec::<Table>::new();
    for d in db_path.as_ref().read_dir()?
    {
        if let Ok(d) = d
        {
            if !d.file_type().unwrap().is_dir() {continue;}
            tables.push(
                open_table(db_path.as_ref(), d.file_name().into_string().unwrap())?
            )
        }
    }
    Ok(DB::new(db_path, tables))

}

pub struct BlockSizeIter
{
    reader:File
}

impl BlockSizeIter
{
    fn new(reader:File) -> Self
    {
        Self{reader}
    }
}

impl Iterator for BlockSizeIter
{
    type Item = u32;
    fn next(&mut self) -> Option<Self::Item>
    {
        let mut v:u32 = 0;
        match v.from_byte(&mut self.reader)
        {
            Ok(_) => Some(v),
            Err(_) => None
        }

    }
}

pub fn table_size_iterator(table:&Table) -> std::io::Result<BlockSizeIter>
{
    let file = File::open(table.sizes_file_path())?;
    Ok(BlockSizeIter::new(file))
}


#[cfg(test)]
mod test
{
    use super::*;
    use std::path::PathBuf;

    fn sch_path() -> PathBuf {
        PathBuf::from("./test.sch")
    }
    fn cleanup_file()
    {
        let path = sch_path();
        if path.exists()
        {
            std::fs::remove_file(path).unwrap();
        }
    }
    #[test]
    fn rw_schema()
    {
        cleanup_file();
        let sch = Schema::from(vec![
            ColumnHeader::new("test", TypeName::DBString),
            ColumnHeader::new("f", TypeName::DBInt),
            ColumnHeader::new("ff", TypeName::DBFloat),
        ]);
        let p = sch_path();
        write_schema(&p, &sch).unwrap();

        let rsch = read_schema(&p).unwrap();
        assert_eq!(sch, rsch);

        cleanup_file();

    }
    #[test]
    fn open_table_test()
    {
        let sch = Schema::from(vec![
            ColumnHeader::new("test", TypeName::DBString),
            ColumnHeader::new("f", TypeName::DBInt),
            ColumnHeader::new("ff", TypeName::DBFloat),
        ]);
        make_test_table_dir("test_db", &Table::new("", "test_tb", sch.clone())).unwrap();
        let tb = open_table("test_db", "test_tb").unwrap();
        assert_eq!(tb.path(), PathBuf::from("test_db").join("test_tb"));
        assert_eq!(tb.name(), "test_tb");
        assert_eq!(*tb.schema(), sch);

        remove_dir_all("test_db").unwrap_or_default();
    }

    #[test]
    fn open_db_test()
    {
        let sch1 = Schema::from(vec![
            ColumnHeader::new("test", TypeName::DBString),
            ColumnHeader::new("f", TypeName::DBInt),
            ColumnHeader::new("ff", TypeName::DBFloat),
        ]);
        let sch2 = Schema::from(vec![
            ColumnHeader::new("first", TypeName::DBInt),
            ColumnHeader::new("second", TypeName::DBInt),
            ColumnHeader::new("third", TypeName::DBFloat),
        ]);
        let base_path = PathBuf::from("test_db2");
        let tables = vec![
                Table::new(base_path.join("test1"), "test1", sch1.clone()),
                Table::new(base_path.join("test2"), "test2", sch2.clone())
        ];
        make_test_database("test_db2", &tables).unwrap();
        let db = open_database("test_db2").unwrap();

        assert_eq!(db.get_table("test1"), Some(&tables[0]));
        assert_eq!(db.get_table("test2"), Some(&tables[1]));
        assert_eq!(db.get_table("test3"), None);

        remove_dir_all("test_db2").unwrap_or_default();
    }
}