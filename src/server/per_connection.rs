use crate::mysql::pool::{MysqlConnectionInfo, ConnectionsPoolPlatform};
use std::time::Duration;
use crate::mysql::Result;
use tokio::time::delay_for;
use crate::server::sql_parser::SqlStatement;
use crate::MyError;

/// mysql connection
#[derive(Debug)]
pub struct PerMysqlConn{
    pub conn_info: Option<MysqlConnectionInfo>,
    pub conn_state: bool,
    pub cur_db: String,
    pub cur_autocommit: bool
}

impl PerMysqlConn {
    pub fn new() -> PerMysqlConn {
        PerMysqlConn{ conn_info: None, conn_state: false, cur_db: "information_schema".to_string(), cur_autocommit: false }
    }

    /// 检查连接空闲状态， 空闲超过指定时间且无事务操作则归还连接到连接池
    pub async fn health(&mut self, pool: &mut ConnectionsPoolPlatform) -> Result<()> {
        loop {
            if let Some(conn) = &mut self.conn_info{
                if conn.check_cacke_sleep(){
                    conn.reset_conn_default()?;
                    let new_conn = conn.try_clone()?;
                    pool.return_pool(new_conn, 0).await?;
                    self.conn_info = None;
                    self.conn_state = false;
                    self.cur_db = "information_schema".to_string();
                    self.cur_autocommit = false;
                    break;
                }
            }
            delay_for(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    pub async fn check(&mut self,  pool: &mut ConnectionsPoolPlatform, key: &String, db: &Option<String>, auto_commit: &bool, sql_type: &SqlStatement,seq: u8) -> Result<()> {
        if !self.conn_state{
            self.check_get(pool, key, db, auto_commit, sql_type).await?;
        }else {
            if let Some(conn) = &self.conn_info{
                //检查当前语句是否使用当前连接
                if pool.conn_type_check(&conn.host_info, sql_type).await?{
                    self.check_default_db_and_autocommit(db, auto_commit).await?;
                }else {
                    //不能使用，则需要重新获取连接， 先归还当前连接到连接池
                    let new_conn = conn.try_clone()?;
                    pool.return_pool(new_conn, seq).await?;
                    self.conn_state = false;
                    self.check_get(pool, key, db, auto_commit, sql_type).await?;
                }
            }
        }
        Ok(())
    }


    async fn check_get(&mut self, pool: &mut ConnectionsPoolPlatform, key: &String, db: &Option<String>, auto_commit: &bool, sql_type: &SqlStatement) -> Result<()>{
        let conn = pool.get_pool(sql_type,key).await?;
        //let conn = pool.get_pool(key).await?;
        self.conn_info = Some(conn);
        self.set_default_info(db, auto_commit).await?;
        self.conn_state = true;
        Ok(())
    }

    /// 检查是否存在未提交事务
    pub async fn check_have_transaction(&self) -> Result<()>{
        match &self.conn_info {
            Some(conn) =>{
                if conn.is_transaction{
                    let err = String::from("must commit outstanding transactions");
                    return Err(Box::new(MyError(err.into())));
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn set_default_info(&mut self, db: &Option<String>, auto_commit: &bool) -> Result<()> {
        match &mut self.conn_info {
            Some(conn) => {
                match db{
                    Some(v) => {
                        conn.set_default_db(v.clone())?;
                        self.cur_db = v.clone();
                    }
                    None => {
                        conn.set_default_db("information_schema".to_string())?;
                        self.cur_db = "information_schema".to_string();
                    }
                }
                if *auto_commit{
                    conn.set_default_autocommit(1)?;
                    self.cur_autocommit = true;
                }else {
                    conn.set_default_autocommit(0)?;
                    self.cur_autocommit = false;
                }
            }
            None => {}
        }
        Ok(())
    }

    async fn check_default_db_and_autocommit(&mut self, db: &Option<String>, auto_commit: &bool) -> Result<()>{
        match &mut self.conn_info {
            Some(conn) => {
                match db{
                    Some(v) => {
                        if v != &self.cur_db{
                            conn.set_default_db(v.clone())?;
                            self.cur_db = v.clone();
                        }
                    }
                    None => {
                        if &self.cur_db != &"information_schema".to_string(){
                            conn.set_default_db("information_schema".to_string())?;
                            self.cur_db = "information_schema".to_string();
                        }
                    }
                }
                if auto_commit != &self.cur_autocommit{
                    if *auto_commit{
                        conn.set_default_autocommit(1)?;
                        self.cur_autocommit = true;
                    }else {
                        conn.set_default_autocommit(0)?;
                        self.cur_autocommit = false;
                    }
                }
            }
            None => {}
        }
        Ok(())
    }

    pub async fn return_connection(&mut self, pool: &mut ConnectionsPoolPlatform, seq: u8) -> Result<()> {
        if let Some(conn) = &mut self.conn_info{
            conn.reset_cached().await?;
            conn.reset_conn_default()?;
            let new_conn = conn.try_clone()?;
            pool.return_pool(new_conn, seq).await?;
            self.conn_info = None;
            self.conn_state = false;
        }
        Ok(())
    }

    /// mysql发生异常，关闭连接
    pub async fn reset_connection(&mut self, pool: &mut ConnectionsPoolPlatform, seq: u8) -> Result<()>{
        if let Some(conn) = &mut self.conn_info{
            let new_conn = conn.try_clone()?;
            pool.return_pool(new_conn, seq).await?;
            self.conn_info = None;
            self.conn_state = false;
        }
        Ok(())
    }

    pub async fn get_connection_host_info(&self) -> String {
        let mut host_info = String::from("");
        if let Some(conn) = &self.conn_info{
            host_info = conn.host_info.clone();
        }
        host_info
    }

}