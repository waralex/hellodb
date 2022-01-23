pub mod types;
pub mod io;
pub mod columns;
pub mod blocks;
pub mod db;
pub mod functions;
pub mod execute;
#[cfg(test)]
pub mod test_misc;

pub type DBResult<T> = Result<T, String>;