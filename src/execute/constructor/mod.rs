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
        let offset = Self::parse_offset(&query.offset);

        match &query.body {
                SetExpr::Select(s) => self.parse_select(&s.as_ref(), offset, limit, &query.order_by),
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

    fn parse_offset(off_expr:&Option<Offset>) -> Option<usize>
    {
        match off_expr {
            Some(
                Offset{
                    value:Expr::Value(Value::Number(v, _)), rows
                }
            ) => Some(v.parse::<usize>().unwrap()),
            _ => None
        }
    }

    fn parse_select(&self, select:&Select, offset:Option<usize>, limit:Option<usize>, order:&Vec<OrderByExpr>) -> DBResult<Plan>
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

        let mut order_field_names = Vec::<String>::new();
        for order_expr in order
        {
            let col_name = expr_constr.parse(&order_expr.expr)?;
            order_field_names.push(col_name);
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

        let mut order_fields = Vec::<(usize, bool)>::new();

        for (order_expr, col_name) in order.iter().zip(order_field_names)
        {
            let col_index = input.col_index_by_name(&col_name).unwrap();
            let is_asc = order_expr.asc.unwrap_or(true);
            order_fields.push((col_index, is_asc));
        }


        let mut step = ExecuteStep::new(input, output);
        step.add_proc(
            ChunkedProcessor::new_ref(table_size_iterator(&table).unwrap())
        );

        let has_order = !order_fields.is_empty();

        let copy_offset = if has_order {None} else {offset};
        let copy_limit = if has_order {None} else {limit};

        let append_proc_ref = FilteredAppendToOutputProcessor::new_ref(
                    res_indexes,
                    filter_col_index,
                    copy_offset,
                    copy_limit
                );
        step.add_proc(append_proc_ref.clone());


        if has_order
        {
            step.add_post_proc(OrderByPostProcessor::new_ref(order_fields, offset, limit));
        }
        else
        {
            step.add_post_proc(append_proc_ref.clone());
        }

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

    #[test]
    fn make_plan()
    {
        cleanup_test_table("constr_db");
        let db = create_test_db("constr_db", 100);

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

        cleanup_test_table("constr_db");
    }

}
