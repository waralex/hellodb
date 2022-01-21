pub mod constructor;
mod steps;
use crate::blocks::{BlockRef, ColumnBlock};
use steps::*;
use steps::processor::*;
use cli_table::TableStruct;

//TODO While simple of one step. In the future, it needs to be expanded to add sequential steps
pub struct Plan
{
    step :ExecuteStep,
    result :BlockRef
}

impl Plan
{
    pub fn new(step:ExecuteStep, result :BlockRef) -> Self
    {
        Self{step, result}
    }

    pub fn result_cli_table(&self, max_rows:usize) -> TableStruct
    {
        let bl = &self.result.borrow();
        bl.cli_table(max_rows)
    }
}