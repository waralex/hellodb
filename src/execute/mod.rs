mod constructor;
mod steps;


use crate::blocks::{ColumnBlock, BlockRef};
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

    pub fn output(&self) -> BlockRef
    {
        self.step.output().clone()
    }

    pub fn result_cli_table(&self, max_rows:usize) -> TableStruct
    {
        let out = self.step.output();
        let bl = out.borrow();
        bl.cli_table(max_rows)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_misc::*;
    use crate::types::types::*;
    use cli_table::print_stdout;

    #[test]
    fn limit()
    {
        cleanup_test_table("plan_db");
        let db = create_test_db("plan_db", 1000);
        let mut plan = Plan::from_sql(&db, "select * from regs limit 10").unwrap();
        plan.execute().unwrap();
        let out_block_ref = plan.output();
        let out_block = out_block_ref.borrow();
        assert_eq!(out_block.rows_len(), 10);

        let mut plan = Plan::from_sql(&db, "select * from regs where gender = 'Female' limit 10").unwrap();
        plan.execute().unwrap();
        let out_block_ref = plan.output();
        let out_block = out_block_ref.borrow();
        assert_eq!(out_block.rows_len(), 10);

        let mut plan = Plan::from_sql(&db, "select id from regs  limit 5 offset 5").unwrap();
        plan.execute().unwrap();
        let out_block_ref = plan.output();
        let out_block = out_block_ref.borrow();
        assert_eq!(out_block.rows_len(), 5);
        assert_eq!(out_block.col_at(0).downcast_data_ref::<DBInt>().unwrap()[0], 6);
        cleanup_test_table("plan_db");
    }

    #[test]
    fn order_by()
    {
        cleanup_test_table("order_db");
        let db = create_test_db("order_db", 10);
        let mut plan = Plan::from_sql(&db, "select id, gender, age from regs order by gender desc, age limit 5 offset 2 ").unwrap();
        plan.execute().unwrap();

        let out_block_ref = plan.output();
        let out_block = out_block_ref.borrow();
        assert_eq!(out_block.rows_len(), 5);
        assert_eq!(
            out_block.col_at(0).downcast_data_ref::<DBInt>().unwrap().as_ref(),
            vec![5 as i64, 3, 1, 10, 8]
            );
        cleanup_test_table("order_db");
    }
}