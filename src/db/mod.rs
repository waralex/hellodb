pub mod table;
use table::Table;
use std::collections::HashMap;
use std::path::{PathBuf, Path};
pub struct DB
{
    path:PathBuf,
    tables:HashMap<String,Table>
}

impl DB
{
    pub fn new(path: impl AsRef<Path>, tables_vec:Vec<Table>) -> DB
    {
        let mut tables = HashMap::<String, Table>::new();
        for t in tables_vec
        {
            tables.insert(t.name().to_string(), t);
        }
        DB{path:PathBuf::from(path.as_ref()), tables}
    }

    pub fn get_table(&self, name:impl AsRef<str>) -> Option<&Table>
    {
        self.tables.get(name.as_ref())
    }
}
