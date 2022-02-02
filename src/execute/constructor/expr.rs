use super::*;
use crate::blocks::source::*;
use std::fs::File;
use crate::functions::regular::arithmetic::*;
use crate::functions::regular::cmp::*;
use crate::functions::regular::boolean::*;
use crate::columns::Column;
use crate::columns::header::ColumnHeader;
use crate::types::TypeName;
use crate::types::types::*;

pub struct ExprConstructor<'a>
{
   table:&'a Table,
   input:&'a mut ColumnBlock
}

impl<'a> ExprConstructor<'a>
{
    pub fn new(table:&'a Table, input:&'a mut ColumnBlock) -> Self
    {
        Self{table, input}
    }

    pub fn parse(&mut self, expr:&Expr) -> DBResult<String>
    {
        let mut col_name = format!("{}", expr);

        if self.input.has_col(&col_name) {return Ok(col_name);}

        match expr {
            Expr::Identifier(v) => {
                self.parse_ident(&v)?;
            },
            Expr::BinaryOp{left, op, right} => {
                self.parse_binary_op(&col_name, &op, &left, &right)?;
            },
            Expr::UnaryOp{op, expr} => {
                self.parse_unary_op(&col_name, &op, &expr)?;
            },
            Expr::Value(v) => {
                self.parse_value(&col_name, &v)?;
            },
            Expr::Nested(v) => {
                col_name = format!("{}", v);
                self.parse(v)?;
            },
            other => {return Err(format!("{} is not supported yet", other));}
        };

        Ok(col_name)
    }

    pub fn process_wild(&mut self) -> Vec<String>
    {
        let mut res = Vec::<String>::new();
        for head in self.table.schema().headers_ref().iter()
        {
            self.parse_ident(&Ident{value:head.name().to_string(), quote_style:None}).unwrap();
            res.push(head.name().to_string());
        }

        res
    }
    fn parse_ident(&mut self, ident:&Ident) -> DBResult<()>
    {
        match self.table.make_column(&ident.value) {
            Some(c) => {
                let type_name =c.type_name();
                self.input.add(
                    c,
                    ExternalSource::new_ref(
                        File::open(self.table.col_path(&ident.value).unwrap()).unwrap(),
                        type_name
                    )
                );
                Ok(())
            },
            None => {
                Err(
                    format!("Filed {} not found in {}", &ident.value, &self.table.name())
                )
            }
        }
    }
    fn parse_binary_op(&mut self, col_name:&String, op:&BinaryOperator, left:&Expr, right:&Expr) -> DBResult<()>
    {
        let op_builder = match op {
            BinaryOperator::Plus => PlusBuilder::new_ref(),
            BinaryOperator::Minus => MinusBuilder::new_ref(),
            BinaryOperator::Multiply => MultiplyBuilder::new_ref(),
            BinaryOperator::Divide => DivideBuilder::new_ref(),
            BinaryOperator::Eq => EqualBuilder::new_ref(),
            BinaryOperator::NotEq => NotEqualBuilder::new_ref(),
            BinaryOperator::Lt => LessBuilder::new_ref(),
            BinaryOperator::LtEq => LessEqualBuilder::new_ref(),
            BinaryOperator::Gt => GreaterBuilder::new_ref(),
            BinaryOperator::GtEq => GreaterEqualBuilder::new_ref(),
            BinaryOperator::And => AndBuilder::new_ref(),
            BinaryOperator::Or => OrBuilder::new_ref(),
            _ =>  return Err(format!("Operation {} not supported yet", op))
        };

        let left_name = &self.parse(left)?;
        let right_name = &self.parse(right)?;
        let left_index = self.input.col_index_by_name(&left_name).unwrap();
        let right_index = self.input.col_index_by_name(&right_name).unwrap();
        let arg_types = vec![
            self.input.col_at(left_index).type_name(),
            self.input.col_at(right_index).type_name(),
        ];
        let type_name = op_builder.result_type(arg_types.clone())?;
        self.input.add(
            Column::new(ColumnHeader::new(col_name, type_name)),
            FunctionSource::new_ref(
                vec![left_index, right_index],
                op_builder.build(arg_types)?
            )
        );

        Ok(())
    }
    fn parse_unary_op(&mut self, col_name:&String, op:&UnaryOperator, expr:&Expr) -> DBResult<()>
    {
        let op_builder = match op {
            UnaryOperator::Not => NotBuilder::new_ref(),
            _ =>  return Err(format!("Operation {} not supported yet", op))
        };

        let expr_name = &self.parse(expr)?;
        println!("FFF {}", expr_name);
        let expr_index = self.input.col_index_by_name(&expr_name).unwrap();
        let arg_types = vec![
            self.input.col_at(expr_index).type_name(),
        ];
        let type_name = op_builder.result_type(arg_types.clone())?;
        self.input.add(
            Column::new(ColumnHeader::new(col_name, type_name)),
            FunctionSource::new_ref(
                vec![expr_index],
                op_builder.build(arg_types)?
            )
        );
        Ok(())
    }
    fn parse_value(&mut self, col_name:&String, val:&Value) -> DBResult<()>
    {
        let (source, type_name) = match val {
            Value::Number(v, _) => {

                match v.parse::<i64>()
                {
                    Ok(value) => (ConstValueSource::<DBInt>::new_ref(value), TypeName::DBInt),
                    Err(_) => {match v.parse::<f64>() {
                            Ok(value) => (ConstValueSource::<DBFloat>::new_ref(value), TypeName::DBFloat),
                            Err(_) => return Err("number parse error".to_string())
                        }
                    }
                }

            },
            Value::SingleQuotedString(v) | Value::DoubleQuotedString(v) => {
                (ConstValueSource::<DBString>::new_ref(v.clone()), TypeName::DBString)
            },
            _ =>return Err(format!("Value {} not supported yet", col_name))
        };

        self.input.add(
            Column::new(ColumnHeader::new(col_name, type_name)),
            source
        );
        Ok(())

    }

}

