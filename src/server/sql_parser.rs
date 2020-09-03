/*
@author: xiao cai niao
@datetime: 2020/5/30
*/
use tracing::{debug};
use std::cmp::PartialEq;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::*;
use sqlparser::ast::{Statement, ObjectType, ObjectName, Expr, SetExpr, TableFactor, TableWithJoins, ExplainStmt};
use crate::mysql::Result;
use crate::MyError;
use crate::mysql::privileges::TableInfo;


/// 解析sql类型
#[derive(Debug, Clone, PartialEq)]
pub enum SqlStatement {
    SetVariable(String, String),
    Query,              //select、with
    Commit,
    Insert,
    Delete,
    Update,
    Rollback,
    StartTransaction,   //begin、start transaction
    AlterTable,
    Create,
    Drop,
    Show,
    ChangeDatabase,             //use db
    Lock,
    UNLock,
    Comment,
    Default
}
// impl SqlStatement{
//     pub fn parser(&self, sql: &String) -> (SqlStatement, Vec<String>){
//         debug!("{}",crate::info_now_time(String::from("start parser sql")));
//         debug!("{}",format!("parser sql: {}", sql));
//         let sql_vec = self.split_sql(sql);
//         let mut tbl_info = self.get_sql_tb_info(&sql_vec);
//         let stamen_type = match sql_vec[0].as_ref(){
//             "desc" => SqlStatement::Query,
//             "select" => SqlStatement::Query,
//             "with" => SqlStatement::Query,
//             "explain" => SqlStatement::Query,
//             "commit" => SqlStatement::Commit,
//             "set" => self.parser_set(sql),
//             "insert" => SqlStatement::Insert,
//             "delete" => SqlStatement::Delete,
//             "update" => SqlStatement::Update,
//             "rollback" => SqlStatement::Rollback,
//             "begin" => SqlStatement::StartTransaction,
//             "start" => {
//                 if sql_vec[1] == "transaction"{
//                     SqlStatement::StartTransaction
//                 }else {
//                     SqlStatement::Default
//                 }
//             }
//             "alter" => SqlStatement::AlterTable,
//             "create" => SqlStatement::Create,
//             "drop" => {
//                 tbl_info = self.get_drop_tbl_info(&sql_vec);
//                 SqlStatement::Drop
//             }
//             "show" => SqlStatement::Show,
//             "use" => SqlStatement::ChangeDatabase,
//             "lock" => SqlStatement::Lock,
//             "unlock" => SqlStatement::UNLock,
//             _ => {
//                 if sql_vec[0].to_string().starts_with("/*!"){
//                     SqlStatement::Comment
//                 }else {
//                     SqlStatement::Default
//                 }
//             }
//         };
//         return (stamen_type, tbl_info)
//     }
//
//     /// 从sql中获取db、table信息
//     fn get_sql_tb_info(&self, sql_vec: &Vec<String>) -> Vec<String>{
//         let mut list = vec![];
//         for (index, i) in sql_vec.iter().enumerate(){
//             if i == "from" || i == "join" || i == "use" {
//                 if !sql_vec[index+1].starts_with("(") {
//                     let tmp = sql_vec[index+1].clone();
//                     let mut b = tmp.chars();
//                     let mut aa = String::from("");
//                     'a: loop {
//                         if let Some(c) = b.next(){
//                             if c.to_string() == ")" {
//                                 list.push(aa.clone());
//                                 break 'a;
//                             }else if c.to_string() != ","{
//                                 aa.push(c);
//                             }else {
//                                 list.push(aa.clone().replace(";", ""));
//                                 aa = String::from("");
//                             }
//                         }else {
//                             list.push(aa.clone().replace(";", ""));
//                             break 'a;
//                         }
//                     }
//                     //list.push(sql_vec[index+1].clone().replace(")", ""));
//                 }
//             }
//         }
//         list
//     }
//
//     fn get_drop_tbl_info(&self, sql_vec: &Vec<String>) -> Vec<String>{
//         let mut list = vec![];
//         for (index, i) in sql_vec.iter().enumerate(){
//             if i == "database" || i == "table" {
//                 list.push(sql_vec[index+1].clone().replace(";", ""));
//             }
//         }
//         list
//     }
//
//     fn split_sql(&self, sql: &String) -> Vec<String> {
//         let sql = sql.to_lowercase().replace("=", " ");
//         let sql_ver = sql.split(" ");
//         let sql_ver = sql_ver.collect::<Vec<&str>>();
//         let mut tmp: Vec<String> = vec![];
//         for i in &sql_ver{
//             if &i.to_string() != &"".to_string()
//                 && &i.to_string() != &"\t".to_string()
//                 && &i.to_string() != &"\n".to_string()
//                 && &i.to_string() != &"\r".to_string()
//                 && &i.to_string() != &"=".to_string(){
//                 tmp.push(i.to_string().clone())
//             }
//         }
//         return tmp;
//     }
//
//     fn parser_set(&self, sql: &String) -> SqlStatement {
//         let sql_vec = self.split_sql(sql);
//         if sql_vec[1].contains("="){
//             let sql_ver = sql_vec[1].split("=");
//             let sql_ver = sql_ver.collect::<Vec<&str>>();
//             if sql_vec.len() == 2{
//                 return SqlStatement::SetVariable(sql_ver[0].to_string(), sql_ver[1].to_string());
//             }
//             return SqlStatement::SetVariable(sql_vec[1].clone(), sql_vec[2].clone())
//         }else {
//             if sql_vec[2].len() > 1 && sql_vec[2].contains("="){
//                 let sql_ver = sql_vec[2].split("=");
//                 let sql_ver = sql_ver.collect::<Vec<&str>>();
//                 if sql_vec.len() == 2{
//                     return SqlStatement::SetVariable(sql_vec[1].clone(), sql_ver[1].to_string());
//                 }
//             }
//             return SqlStatement::SetVariable(sql_vec[1].clone(), sql_vec[2].to_string());
//         }
//
//     }
// }

impl TableInfo{
    fn new(obj: &ObjectName) -> Option<TableInfo> {
        return if obj.0.len() == 1{
            Some(TableInfo{ database: None, table: Some(obj.0[0].value.clone()) })
        }else if obj.0.len() == 2 {
            Some(TableInfo{ database: Some(obj.0[0].value.clone()), table: Some(obj.0[1].value.clone()) })
        }else {
            None
        }
    }
}


pub fn do_table_info(ast: &Vec<Statement>) -> Result<(Vec<TableInfo>, SqlStatement)>{
    let mut tbl_list = vec![];
    let mut sql_type = SqlStatement::Default;

    fn push_tbl_list(obj: &ObjectName, tbl_list: &mut Vec<TableInfo>) -> Result<()>{
        if let Some(v) = TableInfo::new(obj){
            tbl_list.push(v);
        }else {
            return Err(Box::new(MyError(format!("unsupported syntax for {:?}", obj).into())));
        }
        Ok(())
    }

    fn do_expr(expr: &Expr, tbl: &mut Vec<TableInfo>) -> Result<()>{
        match expr{
            Expr::IsNull(e) => {
                do_expr(e, tbl)?;
            }
            Expr::IsNotNull(e) => {
                do_expr(e, tbl)?;
            }
            Expr::InList { expr, list, negated } => {
                do_expr(expr, tbl)?;
                for e in list{
                    do_expr(expr, tbl)?;
                }
            }
            Expr::Between { expr, negated, low, high } => {
                do_expr(expr, tbl)?;
                do_expr(low, tbl)?;
                do_expr(high, tbl)?;
            }
            Expr::BinaryOp { left, op, right } => {
                do_expr(left, tbl)?;
                do_expr(right, tbl)?;
            }
            Expr::UnaryOp {op, expr } => {
                do_expr(expr, tbl)?;
            }
            Expr::Case { operand, conditions, results, else_result } => {
                if let Some(expr) = operand{
                    do_expr(expr, tbl)?;
                }
                for expr in conditions{
                    do_expr(expr, tbl)?;
                }
                for expr in results{
                    do_expr(expr, tbl)?;
                }
                if let Some(expr) = else_result{
                    do_expr(expr, tbl)?;
                }
            }
            Expr::Cast { expr, data_type } => {
                do_expr(expr, tbl)?;
            }
            Expr::Extract { field, expr } => {
                do_expr(expr, tbl)?;
            }
            Expr::Collate { expr, collation } => {
                do_expr(expr, tbl)?;
            }
            Expr::Nested(n) => {
                do_expr(n, tbl)?;
            }
            Expr::InSubquery { expr, subquery, negated } => {
                let (a, _) = do_table_info(&vec![Statement::Query(subquery.clone())])?;
                tbl.extend(a);
            }
            Expr::Subquery(q) => {
                let (a,_) = do_table_info(&vec![Statement::Query(q.clone())])?;
                tbl.extend(a);
            }
            Expr::Exists(e) => {
                let (a,_) = do_table_info(&vec![Statement::Query(e.clone())])?;
                tbl.extend(a);
            }
            _ => {}
        }
        Ok(())
    }

    if ast.len() == 0 {
        sql_type = SqlStatement::Comment;
    }

    for s in ast{
        match s {
            Statement::Query(a) => {
                sql_type = SqlStatement::Query;
                match &a.body{
                    SetExpr::Query(q) => {
                        let (a, _) = do_table_info(&vec![Statement::Query(q.clone())])?;
                        tbl_list.extend(a);
                    }
                    SetExpr::Select(s) => {
                        fn do_withjoin(w: &TableWithJoins, tbl: &mut Vec<TableInfo>) -> Result<()>{
                            get_relation(&w.relation, tbl)?;
                            for j in &w.joins{
                                get_relation(&j.relation, tbl)?;
                            }
                            Ok(())
                        }

                        fn get_relation(r: &TableFactor, tbl: &mut Vec<TableInfo>) -> Result<()>{
                            match r{
                                TableFactor::Table { name, alias, args, with_hints } => {
                                    push_tbl_list(name, tbl)?;
                                    for expr in args{
                                        do_expr(expr, tbl)?;
                                    }
                                    for expr in with_hints{
                                        do_expr(expr, tbl)?;
                                    }

                                }
                                TableFactor::Derived { lateral, subquery, alias } => {
                                    let (a, _) = do_table_info(&vec![Statement::Query(subquery.clone())])?;
                                    tbl.extend(a);
                                }
                                TableFactor::NestedJoin(w) => {
                                    do_withjoin(w, tbl)?;
                                }
                                _ => {}
                            }
                            Ok(())
                        }

                        for t in &s.from{
                            do_withjoin(t, &mut tbl_list)?;
                        }

                        if let Some(v) = &s.selection{
                            do_expr(v, &mut tbl_list)?;
                        }
                    }
                    _ => {}
                }
            }
            Statement::Explain { analyze, format_type, body } => {
                sql_type = SqlStatement::Query;
                match &body{
                    ExplainStmt::Stmt(a) => {
                        let (a, _) = do_table_info(&vec![*a.clone()])?;
                        tbl_list.extend(a);
                    }
                    _ => {}
                }
            }
            Statement::ChangeDatabase {database} => {
                sql_type = SqlStatement::ChangeDatabase;
                tbl_list.push(TableInfo{database: Some(database.clone()), table:None});
            }
            Statement::Lock { lock_tables } => {
                sql_type = SqlStatement::Lock;
                for lock_info in lock_tables{
                    tbl_list.push(TableInfo{database: None, table: Some(lock_info.table_name.to_string().clone())});
                }
            }
            Statement::Drop { object_type, if_exists, names, on_info, cascade } => {
                sql_type = SqlStatement::Drop;
                match object_type{
                    ObjectType::Schema => {
                        for obj in names {
                            push_tbl_list(obj, &mut tbl_list)?;
                        }
                    }
                    ObjectType::Table => {
                        for obj in names{
                            push_tbl_list(obj, &mut tbl_list)?;
                        }
                    }
                    ObjectType::Index => {
                        push_tbl_list(on_info, &mut tbl_list)?;
                    }
                    _ => {}
                }
            }
            Statement::Update { table_name, assignments, selection } => {
                sql_type = SqlStatement::Update;
                push_tbl_list(table_name, &mut tbl_list)?;
                if let Some(se) = selection{
                    do_expr(se, &mut tbl_list)?;
                }
            }
            Statement::Delete { table_name, selection } => {
                sql_type = SqlStatement::Delete;
                push_tbl_list(table_name, &mut tbl_list)?;
                if let Some(se) = selection{
                    do_expr(se, &mut tbl_list)?;
                }
            }
            Statement::AlterTable { name, operation } => {
                sql_type = SqlStatement::AlterTable;
                push_tbl_list(name, &mut tbl_list)?;
            }
            Statement::Insert { table_name, columns, source } => {
                sql_type = SqlStatement::Insert;
                push_tbl_list(table_name, &mut tbl_list)?;
                let (a, _) = do_table_info(&vec![Statement::Query(source.clone())])?;
                tbl_list.extend(a);
            }
            Statement::ShowColumns { extended, full, table_name, filter } => {
                sql_type = SqlStatement::Show;
                push_tbl_list(table_name, &mut tbl_list)?;
            }
            Statement::ShowCreate{table_name} => {
                sql_type = SqlStatement::Show;
                push_tbl_list(table_name, &mut tbl_list)?;
            }
            Statement::Desc { table_name } => {
                sql_type = SqlStatement::Show;
                push_tbl_list(table_name, &mut tbl_list)?;
            }
            Statement::CreateTable { name, columns, constraints, with_options,
                if_not_exists, external, file_format, location,
                query, without_rowid } => {
                sql_type = SqlStatement::Create;
                push_tbl_list(name, &mut tbl_list)?;
                if let Some(q) = query{
                    let (a, _) = do_table_info(&vec![Statement::Query(q.clone())])?;
                    tbl_list.extend(a);
                }

            }
            Statement::CreateIndex { name, table_name, columns, unique, if_not_exists } => {
                sql_type = SqlStatement::Create;
                push_tbl_list(table_name, &mut tbl_list)?;
            }
            Statement::CreateSchema { schema_name } => {
                sql_type = SqlStatement::ChangeDatabase;
                push_tbl_list(schema_name, &mut tbl_list)?;
            }
            Statement::SetVariable { local, variable, value } => {
                sql_type = SqlStatement::SetVariable(variable.value.clone(), format!("{}", value));
            }
            Statement::StartTransaction { modes } => {
                sql_type = SqlStatement::StartTransaction;
            }
            Statement::Rollback { chain } => {
                sql_type = SqlStatement::Rollback;
            }
            Statement::Call { name, parameter } => {
                sql_type = SqlStatement::Query;
            }
            Statement::Commit { chain } => {
                sql_type = SqlStatement::Commit;
            }
            Statement::ShowVariable { variable, global, selection } => {
                sql_type = SqlStatement::Show;
            }
            _ => {}
        }
    }
    Ok((tbl_list, sql_type))
}



// #[cfg(test)]
// mod tests {
//     use crate::server::sql_parser::SqlStatement;
//     use crate::server::sql_parser::do_table_info;
//     use sqlparser::dialect::MySqlDialect;
//     use sqlparser::parser::*;
//
//     #[test]
//     fn test_parse_mysql_sql_db() {
//         let sql = String::from("select * from (select * from t1)a");
//         let dialect = MySqlDialect {};
//         let sql_ast = Parser::parse_sql(&dialect, &sql).unwrap();
//         println!("{:?}", sql_ast);
//         let (a, b) = do_table_info(&sql_ast).unwrap();
//         println!("{:?}", &a);
//     }
//
//     #[test]
//     fn test_parse_sql() {
//         let sql = String::from("select username from a order by b limit 1");
//         let (a, b) = SqlStatement::Default.parser(&sql);
//         assert_eq!(SqlStatement::Query,a);
//         println!("{:?}", &a);
//         println!("{:?}", &b);
//     }
//
//     #[test]
//     fn test_parse_sql_set_names() {
//         let sql = String::from("set names utf8mb4");
//         let (a, b) = SqlStatement::Default.parser(&sql);
//         println!("tables_info: {:?}", &b);
//         match a{
//             SqlStatement::SetVariable(c, d) => {
//                 println!("set: {} = {}", &c, &d);
//             }
//             _ => {}
//         }
//     }
//
//     #[test]
//     fn test_parse_sql_set_autocommit() {
//         let sql = String::from("set autocommit=1");
//         let (a, b) = SqlStatement::Default.parser(&sql);
//         println!("tables_info: {:?}", &b);
//         match a{
//             SqlStatement::SetVariable(c, d) => {
//                 println!("set: {} = {}", &c, &d);
//             }
//             _ => {}
//         }
//     }
//
//     #[test]
//     fn test_parse_join() {
//         let sql = String::from("select a.*,b.id from test a join db1.test b on a.id = b.id where a.id = 10");
//         let (a, b) = SqlStatement::Default.parser(&sql);
//         assert_eq!(SqlStatement::Query,a);
//         println!("{:?}", &a);
//         println!("{:?}", &b);
//     }
//
//     #[test]
//     fn test_parse_sub() {
//         let sql = String::from("select * from (select * from a,b)c;");
//         let (a, b) = SqlStatement::Default.parser(&sql);
//         assert_eq!(SqlStatement::Query,a);
//         println!("{:?}", &a);
//         println!("{:?}", &b);
//     }
//
//     #[test]
//     fn test_drop() {
//         let sql = String::from("drop table abc;");
//         let (a, b) = SqlStatement::Default.parser(&sql);
//         println!("{:?}", &a);
//         println!("{:?}", &b);
//     }
// }
