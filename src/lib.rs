pub mod types;
pub mod io;
pub mod columns;
pub mod blocks;
pub mod db;
pub mod functions;
pub mod execute;

pub type DBResult<T> = Result<T, String>;