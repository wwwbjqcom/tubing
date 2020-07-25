/*
@author: xiao cai niao
@datetime: 2020/5/30
*/
use tracing::{debug};


/// 解析sql类型
#[derive(Debug, Clone)]
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
    Default
}
impl SqlStatement{
    pub fn parser(&self, sql: &String) -> (SqlStatement, Vec<String>){
        debug!("{}",crate::info_now_time(String::from("start parser sql")));
        debug!("{}",format!("parser sql: {}", sql));
        let sql_vec = self.split_sql(sql);
        let tbl_info = self.get_sql_tb_info(&sql_vec);
        let stamen_type = match sql_vec[0].as_ref(){
            "desc" => SqlStatement::Query,
            "select" => SqlStatement::Query,
            "with" => SqlStatement::Query,
            "explain" => SqlStatement::Query,
            "commit" => SqlStatement::Commit,
            "set" => self.parser_set(sql),
            "insert" => SqlStatement::Insert,
            "delete" => SqlStatement::Delete,
            "update" => SqlStatement::Update,
            "rollback" => SqlStatement::Rollback,
            "begin" => SqlStatement::StartTransaction,
            "start" => {
                if sql_vec[1] == "transaction"{
                    SqlStatement::StartTransaction
                }else {
                    SqlStatement::Default
                }
            }
            "alter" => SqlStatement::AlterTable,
            "create" => SqlStatement::Create,
            "drop" => SqlStatement::Drop,
            "show" => SqlStatement::Show,
            "use" => SqlStatement::ChangeDatabase,
            _ => SqlStatement::Default
        };
        return (stamen_type, tbl_info)
    }

    /// 从sql中获取db、table信息
    fn get_sql_tb_info(&self, sql_vec: &Vec<String>) -> Vec<String>{
        let mut list = vec![];
        for (index, i) in sql_vec.iter().enumerate(){
            if i == "from" || i == "join" || i == "use" {
                list.push(sql_vec[index+1].clone());
            }
        }
        list
    }

    fn split_sql(&self, sql: &String) -> Vec<String> {
        let sql = sql.to_lowercase().replace("=", " ");
        let sql_ver = sql.split(" ");
        let sql_ver = sql_ver.collect::<Vec<&str>>();
        let mut tmp: Vec<String> = vec![];
        for i in &sql_ver{
            if &i.to_string() != &"".to_string()
                && &i.to_string() != &"\t".to_string()
                && &i.to_string() != &"\n".to_string()
                && &i.to_string() != &"\r".to_string()
                && &i.to_string() != &"=".to_string(){
                tmp.push(i.to_string().clone())
            }
        }
        return tmp;
    }

    fn parser_set(&self, sql: &String) -> SqlStatement {
        let sql_vec = self.split_sql(sql);
        if sql_vec[1].contains("="){
            let sql_ver = sql_vec[1].split("=");
            let sql_ver = sql_ver.collect::<Vec<&str>>();
            if sql_vec.len() == 2{
                return SqlStatement::SetVariable(sql_ver[0].to_string(), sql_ver[1].to_string());
            }
            return SqlStatement::SetVariable(sql_vec[1].clone(), sql_vec[2].clone())
        }else {
            if sql_vec[2].len() > 1 && sql_vec[2].contains("="){
                let sql_ver = sql_vec[2].split("=");
                let sql_ver = sql_ver.collect::<Vec<&str>>();
                if sql_vec.len() == 2{
                    return SqlStatement::SetVariable(sql_vec[1].clone(), sql_ver[1].to_string());
                }
            }
            return SqlStatement::SetVariable(sql_vec[1].clone(), sql_vec[2].to_string());
        }

    }
}
