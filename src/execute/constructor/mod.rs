use super::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::*;
use crate::DBResult;
use crate::db::DB;
use crate::db::table::Table;
mod expr;
use expr::ExprConstructor;
use crate::execute::steps::processor::*;
use crate::blocks::source::*;
use crate::io::db::table_size_iterator;

pub struct Constructor<'a>
{
   db:&'a DB
}

impl<'a>  Constructor<'a>
{

    pub fn new(db:&'a DB) -> Self
    {
        Self{db}
    }

    pub fn make_plan(&self, sql:&str) -> DBResult<Plan>
    {
        let st = &Self::parse_sql(sql)?[0];
        match &st {
            Statement::Query(v) => self.parse_query(&v),
            other => Err(format!("{} unsupported yet", other))
        }
    }

    fn parse_query(&self, query:&Query) -> DBResult<Plan>
    {
        let limit = Self::parse_limit(&query.limit);
        match &query.body {
                SetExpr::Select(s) => self.parse_select(&s.as_ref(), limit),
                other => Err(format!("{} unsupported yet", other))
            }

    }

    fn parse_limit(lim_expr:&Option<Expr>) -> Option<usize>
    {
        match lim_expr {
            Some(Expr::Value(Value::Number(v, _))) => Some(v.parse::<usize>().unwrap()),
            _ => None
        }
    }

    fn parse_select(&self, select:&Select, limit:Option<usize>) -> DBResult<Plan>
    {
        let table = self.parse_from(&select.from)?;
        let mut input = ColumnBlock::new();
        let mut expr_constr = ExprConstructor::new(&table, &mut input);
        let filter_col_name = match &select.selection {
            Some(e) => Some(expr_constr.parse(&e)?),
            None => None
        };


        let mut res_cols = Vec::<String>::new();

        for itm in select.projection.iter()
        {
            match itm {
                SelectItem::UnnamedExpr(e) => {
                    res_cols.push(expr_constr.parse(&e)?);
                },
                SelectItem::Wildcard => {
                    res_cols.append(&mut expr_constr.process_wild());
                },
                other => {return Err(format!("{} is not supported yet", other));}
            }

        }

        let filter_col_index = match filter_col_name
        {
            Some(n) => Some(input.col_index_by_name(&n).unwrap()),
            None => None
        };

        let mut output = ColumnBlock::new();
        let mut res_indexes = Vec::<usize>::new();
        for rcol in res_cols.iter()
        {
            let col_index = input.col_index_by_name(&rcol).unwrap();
            res_indexes.push(col_index);
            let col = input.col_at(col_index);
            output.add(
                col.clone_empty(),
                DontTouchSource::new_ref()
            );
        }

        let mut step = ExecuteStep::new(input, output);
        step.add_proc(
            Box::new(
                ChunkedProcessor::new(table_size_iterator(&table).unwrap())
            )
        );
        step.add_proc(
            Box::new(
                FilteredAppendToOutputProcessor::new(
                    res_indexes,
                    filter_col_index,
                    limit
                )
            )
        );

        Ok(Plan::new(step))
    }


    fn parse_from(&self, from:&Vec<TableWithJoins>) -> DBResult<&Table>
    {
        if from.len() != 1
        {
            return Err("Unexpected number of tables".to_string());
        }
        if from[0].joins.len() > 0
        {
            return Err("Joins unsupported yet".to_string());
        }
        match &from[0].relation {
            TableFactor::Table{name,.. } => {
                match self.db.get_table(&name.0[0].value) {
                    Some(t) => Ok(&t),
                    None => Err(format!("Table {} don't exists", name.0[0].value))
                }
            }
            other => Err(format!("{} unsupported yet", other))
        }
    }


    fn parse_sql(sql:&str) ->DBResult<Vec<Statement>>
    {
        let dialect = GenericDialect {};
        match Parser::parse_sql(&dialect, sql)
        {
            Ok(v) => Ok(v),
            Err(e) => Err(e.to_string())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_misc::*;
    use crate::columns::header::ColumnHeader;
    use crate::db::table::Schema;
    use std::path::PathBuf;
    use crate::types::TypeName;

    #[test]
    fn make_plan()
    {
        cleanup_test_table("plan_db");
        let db = create_test_db("plan_db");

        let constr = Constructor::new(&db);

        assert!(constr.make_plan("ffasd").is_err());
        let test_query = "select id, age from undef";
        assert!(constr.make_plan(test_query).is_err());

        let test_query = "select id, age from regs where wrong_field";
        assert!(constr.make_plan(test_query).is_err());

        let test_query = "select id, age from regs where id + age";
        assert!(!constr.make_plan(test_query).is_err());

        let test_query = "select id, age from regs where id + 100.5";
        assert!(constr.make_plan(test_query).is_err());

        let test_query = "select id, age from regs where id + 100";
        assert!(!constr.make_plan(test_query).is_err());

        println!("====================");
        let test_query = "select id from regs limit 100";
        assert!(!constr.make_plan(test_query).is_err());

    }

}
