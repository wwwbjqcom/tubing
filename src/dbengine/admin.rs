/*
@author: xiao cai niao
@datetime: 2020/7/14
*/

use crate::mysql::Result;
use crate::MyError;
use tracing::{debug, info, error};
use sqlparser::ast::{Statement, Expr, BinaryOperator, SetVariableValue, Value, Ident};
use std::collections::HashMap;

#[derive(Debug)]
pub enum ShowCommand{
    Status,
    Connections,
    Questions,
    Fuse,
    Null
}

#[derive(Debug)]
pub struct ShowStruct{
    pub command: ShowCommand,
    pub platform: Option<String>
}

impl ShowStruct{
    async fn parse(&mut self, variable: String, selection: &Option<Expr>) -> Result<()>{
        debug!("admin_sql: {:?} {:?}",variable, selection);
        match variable.as_ref(){
            "status" => {
                self.command = ShowCommand::Status;
            }
            "connections" => {
                self.command = ShowCommand::Connections;
            }
            "questions" => {
                self.command = ShowCommand::Questions;
            }
            "fuse" => {
                self.command = ShowCommand::Fuse;
            }
            _ => {
                let err = String::from("only support show status/connections/questions/fuse");
                return Err(Box::new(MyError(err.into())));
            }
        }
        self.parse_platform(selection).await?;
        Ok(())
    }

    fn do_ident(&mut self, ide: &Expr, a: String) -> Result<()> {
        match ide{
            Expr::Identifier(i) => {
                if a == String::from("r"){
                    self.platform = Some(format!("{}",i).replace("\"","").replace("'",""));
                }else if a == String::from("l") {
                    if i.value != String::from("platform"){
                        let err = format!("the show command only supports platform as a condition :{}", i);
                        return Err(Box::new(MyError(err.into())));
                    }
                }

            }
            Value => {
                if a == String::from("r"){
                    self.platform = Some(format!("{}",ide).replace("\"","").replace("'",""));
                }else {
                    let err = format!("unsupported syntax {}", ide);
                    return Err(Box::new(MyError(err.into())));
                }
            }
            _ => {
                let err = format!("unsupported syntax {}", ide);
                return Err(Box::new(MyError(err.into())));
            }
        }
        Ok(())
    }

    async fn do_expr(&mut self, selection: &Expr) -> Result<()>{
        match selection{
            Expr::BinaryOp { left, op, right } => {
                match op{
                    BinaryOperator::Eq => {}
                    _ => {
                        let err = format!("the show command only supports platform as a condition :{} {} {}", left, op, right);
                        return Err(Box::new(MyError(err.into())));
                    }
                }

                self.do_ident(left, String::from("l"))?;
                self.do_ident(right, String::from("r"))?;
            }
            _ => {
                let err = format!("unsupported syntax {}", selection);
                return Err(Box::new(MyError(err.into())));
            }
        }
        Ok(())
    }

    async fn parse_platform(&mut self,selection: &Option<Expr>) -> Result<()>{
        if let Some(v) = selection{
            self.do_expr(v).await?;
        }else {
            self.platform = None
        }

        Ok(())
    }

}

#[derive(Debug)]
pub enum SetVariables{
    MaxThread(usize),
    MinThread(usize),
    Auth(u8),
    Fuse(u8),
    Null
}
impl SetVariables{
    async fn set_value(&self, value: &SetVariableValue) -> SetVariables {
        let parse_value = match value {
            SetVariableValue::Literal(v) => {
                match v{
                    Value::Number(a) => {
                        let b: usize = a.parse().unwrap();
                        b
                    }
                    Value::Boolean(b) => {
                        if *b{
                            1
                        }else {
                            0
                        }
                    }
                    _ => {0}
                }
            }
            _ => {
                0
            }
        };


        return match self {
            SetVariables::MaxThread(_) => {
                SetVariables::MaxThread(parse_value)
            }
            SetVariables::MinThread(_) => {
                SetVariables::MinThread(parse_value)
            }
            SetVariables::Auth(_) => {
                SetVariables::Auth(parse_value as u8)
            }
            SetVariables::Fuse(_) => {
                SetVariables::Fuse(parse_value as u8)
            }
            _ => {
                SetVariables::Null
            }
        }
    }
}

#[derive(Debug)]
pub struct SetStruct{
    pub set_variables: SetVariables,
    pub platform: Option<String>,
    pub host_info: Option<String>
}

impl SetStruct{
    async fn parse(&mut self,  variable: String, selection: &Option<Expr>, value: &SetVariableValue) -> Result<()> {
        match variable.as_ref() {
            "min_thread" => {
                self.parse_value(SetVariables::MinThread(0), selection, value).await?;
            }
            "max_thread" => {
                self.parse_value(SetVariables::MaxThread(0), selection, value).await?;
            }
            "auth" => {
                self.parse_value(SetVariables::Auth(0), selection, value).await?;
            }
            "fuse" => {
                self.parse_value(SetVariables::Fuse(0), selection, value).await?;
            }
            _ => {
                let err = String::from("only support set min_thread/max_thread/auth/fuse");
                self.return_error(err).await?;
            }
        }
        Ok(())
    }

    fn do_ident(&mut self, ide: &Expr, vv: &mut String, aa: String) -> Result<()> {
        match ide{
            Expr::Identifier(i) => {
                if aa == String::from("r"){
                    if vv == &String::from("platform"){
                        self.platform = Some(format!("{}",i).replace("\"","").replace("'",""));
                    }else if vv == &String::from("host_info") {
                        self.host_info = Some(format!("{}",i).replace("\"","").replace("'",""));
                    }
                }else if aa == String::from("l") {
                    if i.value == String::from("platform"){
                        *vv = String::from("platform");
                    } else if i.value == String::from("host_info"){
                        *vv = String::from("host_info");
                    }else {
                        let err = format!("unsupported syntax: {} for {}",i.value.to_string(), aa);
                        return Err(Box::new(MyError(err.into())));
                    }
                }

            }
            Value=> {
                if aa == String::from("r"){
                    if vv == &String::from("platform"){
                        self.platform = Some(format!("{}",ide).replace("\"","").replace("'",""));
                    }else if vv == &String::from("host_info") {
                        self.host_info = Some(format!("{}",ide).replace("\"","").replace("'",""));
                    }
                }else {
                    let err = format!("unsupported syntax {}", ide);
                    return Err(Box::new(MyError(err.into())));
                }
            }
            _ => {
                let err = String::from("unsupported syntax");
                return Err(Box::new(MyError(err.into())));
            }
        }
        Ok(())
    }

    async fn do_expr(&mut self, selection: &Expr) -> Result<()>{
        let mut vv = String::from("");
        match selection{
            Expr::BinaryOp { left, op, right } => {
                match op{
                    BinaryOperator::Eq => {}
                    _ => {
                        let err = String::from("unsupported syntax");
                        return Err(Box::new(MyError(err.into())));
                    }
                }
                self.do_ident(left, &mut vv, String::from("l"))?;

                self.do_ident(right, &mut vv, String::from("r"))?;
            }
            _ => {
                let err = String::from("unsupported syntax");
                return Err(Box::new(MyError(err.into())));
            }
        }
        Ok(())
    }

    async fn parse_value(&mut self,set_type: SetVariables, selection: &Option<Expr>, value: &SetVariableValue) -> Result<()>{
        // let value: usize = sql_vec[2].parse()?;
        self.set_variables = set_type.set_value(value).await;
        let mut vv = String::from("");
        if let Some(s) = selection{
            match s{
                Expr::BinaryOp { left, op, right } => {
                    match op{
                        BinaryOperator::Eq => {
                            self.do_ident(left, &mut vv, String::from("l"))?;
                            self.do_ident(right, &mut vv, String::from("r"))?;
                        }
                        BinaryOperator::And =>{
                            self.do_expr(left).await?;
                            self.do_expr(right).await?;
                        }
                        _ => {
                            let err = format!("unsupported syntax {}", op);

                            return Err(Box::new(MyError(err.into())));
                        }
                    }
                }
                _ => {
                    let err = String::from("the set command must provide conditions");
                    return Err(Box::new(MyError(err.into())));
                }
            }
        }
        if let Some(host_info) = &self.host_info{
            match self.platform {
                None => {
                    let err = String::from("when the host_info parameter is provided, the platform parameter must also be provided");
                    return Err(Box::new(MyError(err.into())));
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn return_error(&self, err: String) -> Result<()>{
        return Err(Box::new(MyError(err.into())));
    }

}

#[derive(Debug)]
pub enum AdminSql{
    Show(ShowStruct),
    Set(SetStruct),
    Null
}

impl AdminSql{
    /// 按固定规则解析管理命令， 该sql不支持标准sql规范，为字符串匹配
    pub async fn parse_sql(&self, ast: &Vec<Statement>) -> Result<AdminSql>{
        for a in ast{
            return match a {
                Statement::ShowVariable { variable, global, selection } => {
                    let mut show_struct = ShowStruct { command: ShowCommand::Null, platform: None };
                    show_struct.parse(format!("{}", variable), selection).await?;
                    Ok(AdminSql::Show(show_struct))
                }
                Statement::AdminSetVariable { variable, value, selection } => {
                    info!("aa");
                    let mut set_struct = SetStruct {
                        set_variables: SetVariables::Null,
                        platform: None,
                        host_info: None
                    };
                    set_struct.parse(format!("{}", variable), selection, value).await?;
                    info!("cc");
                    Ok(AdminSql::Set(set_struct))
                }
                _ => {
                    Err(Box::new(MyError(String::from("the admin module only supports set auth/pool/fuse where .. and show status/questions/connections/fuse").into())))
                }
            }
        }
        info!("bb");
        return Err(Box::new(MyError(String::from("unsupported syntax").into())));

    }

}

#[derive(Debug)]
pub struct HostPoolState{
    pub host_info: String,
    pub com_select: usize,
    pub com_update: usize,
    pub com_delete: usize,
    pub com_insert: usize,
    pub min_thread: usize,
    pub max_thread: usize,
    pub thread_count: usize,
    pub active_thread: usize,
    pub cached_count: usize,
    pub auth: bool,
    pub fuse: bool,
    pub platform_conn_count: HashMap<String, usize>,
}

#[derive(Debug)]
pub struct PoolState{
    pub platform: String,
    pub write_host: String,
    pub read_host: Vec<String>,
    pub questions: usize,
    pub host_state: Vec<HostPoolState>
}

#[derive(Debug)]
pub struct ShowState{
    pub platform_state: Vec<PoolState>
}

// #[cfg(test)]
// mod tests {
//
//     use crate::dbengine::admin::{SetStruct, AdminSql};
//     use sqlparser::dialect::MySqlDialect;
//     use sqlparser::parser::*;
//
//     #[test]
//     async fn test_parse_admin_sql() {
//         let sql = "set auth=1 where paltform='a' and host_info='1234'";
//         let dialect = MySqlDialect {};
//         let sql_ast = Parser::parse_sql(&dialect, &sql).unwrap();
//         let admin_sql = AdminSql::Null;
//         let admin_sql = admin_sql.parse_sql(&sql_ast).await.unwrap();
//         println!("{:?}", admin_sql);
//     }
// }
