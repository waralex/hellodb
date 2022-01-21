use super::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::*;
use crate::DBResult;
use crate::db::DB;
use crate::db::table::Table;

pub fn make_plan_by_sql(db:&DB, sql:&str) -> DBResult<()>
{
    let st = &parse_sql(sql)?[0];
    match &st {
        Statement::Query(v) => match &v.body {
            SetExpr::Select(s) => parse_select(&db, &s.as_ref()),
            other => Err(format!("{} unsupported yet", other))
        },
        other => Err(format!("{} unsupported yet", other))
    }
}

pub fn parse_select(db:&DB, select:&Select) -> DBResult<()>
{
    let table = parse_from(&db, &select.from)?;
    Ok(())
}

fn parse_from<'a, 'b>(db:&'a DB, from:&'b Vec<TableWithJoins>) -> DBResult<&'a Table>
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
            match db.get_table(&name.0[0].value) {
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