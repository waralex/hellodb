pub mod header;
pub mod data;

use header::ColumnHeader;
use data::*;

pub struct Column
{

    header :ColumnHeader,
    data :StoragePtr
}

impl Column {

    pub fn new(header:ColumnHeader) -> Self
    {
        let type_name = header.type_name();
        Self{
            header,
            data : make_storage(type_name)
        }
    }

    pub fn header(&self) -> &ColumnHeader
    {
        &self.header
    }

    pub fn data(&self) -> &StoragePtr
    {
        &self.data
    }
    pub fn mut_data(&mut self) -> &mut StoragePtr
    {
        &mut self.data
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::types::TypeName;
    use crate::types::types::*;
    use crate::columns::data::is_storage_of;

    #[test]
    fn new_column()
    {
        let mut c = Column::new(ColumnHeader::new("test", TypeName::Int8));
        assert!(is_storage_of::<Int8>(c.data()));
        let ci8 = downcast_storage_mut::<Int8>(c.mut_data()).unwrap();
        ci8.push(10);
        ci8.push(20);
        let ci8 = downcast_storage_ref::<Int8>(c.data()).unwrap();
        assert_eq!(ci8[0], 10);
        assert_eq!(ci8[1], 20);
    }
}
