use crate::columns::header::ColumnHeader;
use std::path::{PathBuf, Path};

#[derive(Clone, Debug, PartialEq)]
pub struct Schema
{
    headers:Vec<ColumnHeader>
}

impl Schema
{
    //There are few columns and the operation is rare, so there is no point in indexing through a hash map
    pub fn find_col(&self, name:&str) -> Option<&ColumnHeader>
    {
        self.headers.iter().find(|&v| v.name() == name)
    }

    pub fn len(&self) -> usize
    {
        self.headers.len()
    }

    pub fn headers_ref(&self) -> &Vec<ColumnHeader>
    {
        &self.headers
    }
}

impl From<&mut [ColumnHeader]> for Schema {

    fn from(headers: &mut [ColumnHeader]) -> Schema {
        Schema { headers:Vec::from(headers)}
    }
}

impl From<&[ColumnHeader]> for Schema {

    fn from(headers: &[ColumnHeader]) -> Schema {
        Schema { headers:Vec::from(headers)}
    }
}

impl From<Vec<ColumnHeader>> for Schema {

    fn from(headers: Vec<ColumnHeader>) -> Schema {
        Schema { headers:Vec::from(headers)}
    }
}

#[derive(Debug, PartialEq)]
pub struct Table
{
    path:PathBuf,
    name:String,
    schema:Schema,
    rows_len:u64
}

impl Table
{
    pub fn new(path:impl AsRef<Path>, name:&str, schema:Schema, rows_len:u64) -> Table
    {
        Table{path:PathBuf::from(path.as_ref()), name:name.to_string(), schema, rows_len}
    }

    pub fn path(&self) -> &Path
    {
        &self.path
    }

    pub fn name(&self) -> &str
    {
        &self.name
    }

    pub fn schema(&self) -> &Schema
    {
        &self.schema
    }

    pub fn rows_len(&self) -> u64
    {
        self.rows_len
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::types::TypeName;

    #[test]
    fn find_col()
    {
        let sch = Schema::from(vec![
            ColumnHeader::new("test", TypeName::DBString),
            ColumnHeader::new("f", TypeName::DBInt),
            ColumnHeader::new("ff", TypeName::DBFloat),
        ]);

        assert_eq!(sch.find_col("kkk"), None);
        assert_eq!(sch.find_col("f"), Some(&ColumnHeader::new("f", TypeName::DBInt)));
    }
}