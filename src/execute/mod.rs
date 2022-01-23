mod constructor;
mod steps;


use crate::blocks::ColumnBlock;
use crate::DBResult;
use steps::*;
use cli_table::TableStruct;
use crate::db::DB;
use constructor::Constructor;


//TODO While simple of one step. In the future, it needs to be expanded to add sequential steps
pub struct Plan
{
    step :ExecuteStep,
}

impl Plan
{
    pub fn new(step:ExecuteStep) -> Self
    {
        Self{step}
    }

    pub fn from_sql(db:&DB, sql:&str) -> DBResult<Self>
    {
        let constr = Constructor::new(&db);
        constr.make_plan(sql)
    }

    pub fn execute(&mut self) -> DBResult<()>
    {
        self.step.execute()
    }

    pub fn result_cli_table(&self, max_rows:usize) -> TableStruct
    {
        let out = self.step.output();
        let bl = out.borrow();
        bl.cli_table(max_rows)
    }
}