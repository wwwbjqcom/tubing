/*
@author: xiao cai niao
@datetime: 2020/7/14
*/

use crate::mysql::Result;
use crate::MyError;
use tracing::debug;


pub enum ShowCommand{
    Status,
    Connections,
    Questions,
    Null
}

pub struct ShowStruct{
    pub command: ShowCommand,
    pub platform: Option<String>
}
impl ShowStruct{
    async fn parse(&mut self,sql_vec: &Vec<String>) -> Result<()>{
        debug!("admin_sql_vec:{:?}",&sql_vec);
        match sql_vec[1].as_ref(){
            "status" => {
                self.command = ShowCommand::Status;
            }
            "connections" => {
                self.command = ShowCommand::Connections;
            }
            "questions" => {
                self.command = ShowCommand::Questions;
            }
            _ => {
                let err = String::from("only support show status/connections/questions");
                return Err(Box::new(MyError(err.into())));
            }
        }
        self.parse_platform(sql_vec).await?;
        Ok(())
    }

    async fn parse_platform(&mut self,sql_vec: &Vec<String>) -> Result<()>{
        if sql_vec.len() < 2{
            let err = String::from("unsupported syntax");
            return Err(Box::new(MyError(err.into())));
        }else if sql_vec.len() == 2{
            self.platform = None;
        }else if &sql_vec[2] == &String::from("where") && &sql_vec[3] == &String::from("platform"){
            self.platform = Some(sql_vec[4].clone());
        }else {
            let err = String::from("unsupported syntax");
            return Err(Box::new(MyError(err.into())));
        }
        Ok(())
    }

}

pub enum SetVariables{
    MaxThread(usize),
    MinThread(usize),
    Auth(u8),
    Null
}
impl SetVariables{
    async fn set_value(&self, value: usize) -> SetVariables {
        match self{
            SetVariables::MaxThread(_) =>{
                return SetVariables::MaxThread(value);
            }
            SetVariables::MinThread(_) => {
                return SetVariables::MinThread(value);
            }
            SetVariables::Auth(_) => {
                return SetVariables::Auth(value as u8);
            }
            _ => {
                return SetVariables::Null;
            }
        }
    }
}

pub struct SetStruct{
    pub set_variables: SetVariables,
    pub platform: Option<String>,
    pub host_info: Option<String>
}

impl SetStruct{
    async fn parse(&mut self, sql_vec: &Vec<String>) -> Result<()> {
        if sql_vec.len() < 3{
            self.return_error(String::from("unsupported syntax")).await?;
        }
        match sql_vec[1].as_ref(){
            "min_thread" => {
                self.parse_value(sql_vec, SetVariables::MinThread(0)).await?;
            }
            "max_thread" => {
                self.parse_value(sql_vec, SetVariables::MaxThread(0)).await?;
            }
            "auth" => {
                self.parse_value(sql_vec, SetVariables::Auth(0)).await?;
            }
            _ => {
                let err = String::from("only support set min_thread/max_thread/auth");
                self.return_error(err).await?;
            }
        }
        Ok(())
    }

    async fn parse_value(&mut self,sql_vec: &Vec<String>, set_type: SetVariables) -> Result<()>{
        let value: usize = sql_vec[2].parse()?;
        self.set_variables = set_type.set_value(value).await;
        if sql_vec.len() > 3{
            if &sql_vec[3] == &String::from("where"){
                match sql_vec[4].as_ref(){
                    "platform" => {
                        self.platform = Some(sql_vec[5].clone());
                    }
                    "host_info" => {
                        self.host_info = Some(sql_vec[5].clone());
                    }
                    _ => {
                        self.return_error(String::from("unsupported syntax")).await?;
                    }
                }
            }

            if sql_vec.len() == 9 {
                if &sql_vec[6] == &String::from("and") {
                    match sql_vec[7].as_ref(){
                        "paltform" => {
                            if let Some(_) = self.platform{
                                self.return_error(String::from("unsupported syntax")).await?;
                            }
                            self.platform = Some(sql_vec[8].clone());
                        }
                        "host_info" => {
                            if let Some(_) = self.host_info {
                                self.return_error(String::from("unsupported syntax")).await?;
                            }
                            if let None = self.platform {
                                self.return_error(String::from("unsupported syntax")).await?;
                            }
                            self.host_info = Some(sql_vec[8].clone());
                        }
                        _ => {
                            self.return_error(String::from("unsupported syntax")).await?;
                        }
                    }
                }
            }else if sql_vec.len() > 6 {
                self.return_error(String::from("unsupported syntax")).await?;
            }
        }
        Ok(())
    }

    async fn return_error(&self, err: String) -> Result<()>{
        return Err(Box::new(MyError(err.into())));
    }

}

pub enum AdminSql{
    Show(ShowStruct),
    Set(SetStruct),
    Null
}

impl AdminSql{
    /// 按固定规则解析管理命令， 该sql不支持标准sql规范，为字符串匹配
    pub async fn parse_sql(&self,sql: &String) -> Result<AdminSql>{
        let sql_vec = self.split_sql(sql).await;
        match sql_vec[0].as_ref(){
            "show" => {
                let mut show_struct = ShowStruct{ command: ShowCommand::Null, platform: None };
                show_struct.parse(&sql_vec).await?;
                return Ok(AdminSql::Show(show_struct));
            }
            "set" => {
                let mut set_struct = SetStruct{
                    set_variables: SetVariables::Null,
                    platform: None,
                    host_info: None
                };
                set_struct.parse(&sql_vec).await?;
                return Ok(AdminSql::Set(set_struct));
            }
            _ => {
                return Err(Box::new(MyError(String::from("unsupported syntax").into())));
            }
        }
    }

    async fn split_sql(&self, sql: &String) -> Vec<String> {
        let sql = sql.to_lowercase().replace("=", " ").replace("\"","");
        let sql_ver = sql.split(" ");
        let sql_ver = sql_ver.collect::<Vec<&str>>();
        let mut tmp: Vec<String> = vec![];
        for i in &sql_ver{
            if &i.to_string() != &"".to_string()
                && &i.to_string() != &"\t".to_string()
                && &i.to_string() != &"\n".to_string()
                && &i.to_string() != &"\r".to_string(){
                tmp.push(i.to_string().clone())
            }
        }
        return tmp;
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
    pub auth: bool
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
