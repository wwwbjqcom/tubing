use crate::mysql::pool::{MysqlConnectionInfo, ConnectionsPoolPlatform, ConnectionsPool};
use std::time::Duration;
use crate::mysql::Result;
use tokio::time::delay_for;
use crate::server::sql_parser::SqlStatement;
use crate::MyError;

/// mysql connection
#[derive(Debug)]
pub struct PerMysqlConn{
    pub conn_info: Option<MysqlConnectionInfo>,
    pub conn_pool: Option<ConnectionsPool>,
    pub conn_state: bool,
    pub cur_db: String,
    pub cur_autocommit: bool
}

impl PerMysqlConn {
    pub fn new() -> PerMysqlConn {
        PerMysqlConn{ conn_info: None, conn_pool: None, conn_state: false, cur_db: "information_schema".to_string(), cur_autocommit: false }
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

    pub async fn check(&mut self,  pool: &mut ConnectionsPoolPlatform, key: &String,
                       db: &Option<String>, auto_commit: &bool,
                       sql_type: &SqlStatement,seq: u8, select_comment: Option<String>) -> Result<()> {
        if !self.conn_state{
            self.check_get(pool, key, db, auto_commit, sql_type, &select_comment).await?;
        }else {
            if let Some(conn) = &self.conn_info{
                //检查当前语句是否使用当前连接
                if pool.conn_type_check(&conn.host_info, sql_type, &select_comment).await?{
                    self.check_default_db_and_autocommit(db, auto_commit).await?;
                }else {
                    //不能使用，则需要重新获取连接， 先归还当前连接到连接池
                    // let new_conn = conn.try_clone()?;
                    self.return_connection(seq).await?;
                    // pool.return_pool(new_conn, seq).await?;
                    // self.conn_state = false;
                    self.check_get(pool, key, db, auto_commit, sql_type, &select_comment).await?;
                }
            }
        }
        Ok(())
    }

    async fn check_get(&mut self, pool: &mut ConnectionsPoolPlatform, key: &String, db: &Option<String>,
                       auto_commit: &bool, sql_type: &SqlStatement, select_comment: &Option<String>) -> Result<()>{
        let (conn, conn_pool) = pool.get_pool(sql_type,key, select_comment).await?;
        //let conn = pool.get_pool(key).await?;
        self.conn_info = Some(conn);
        self.conn_pool = Some(conn_pool);
        self.set_default_info(db, auto_commit).await?;
        self.conn_state = true;
        Ok(())
    }

    /// 判断是否开启审计，开启则打印sql
    pub async fn check_auth_save(&mut self, sql: &String, host: &String) {
        if let Some(conn_pool) = &self.conn_pool{
            conn_pool.auth_save(sql, host).await;
        }
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

    // pub async fn return_connection(&mut self, pool: &mut ConnectionsPoolPlatform, seq: u8) -> Result<()> {
    //     if let Some(conn) = &mut self.conn_info{
    //         let new_conn = conn.try_clone()?;
    //         if let Some(conn_pool) = &mut self.conn_pool{
    //             conn_pool.return_pool();
    //         }
    //         pool.return_pool(new_conn, seq).await?;
    //         self.conn_info = None;
    //         self.conn_state = false;
    //     }
    //     Ok(())
    // }

    pub async fn return_connection(&mut self, seq: u8) -> Result<()> {
        if let Some(conn) = &mut self.conn_info{
            let mut new_conn = conn.try_clone()?;
            // 如果有事务存在则回滚， 回滚失败会结束该连接
            new_conn.check_rollback(seq).await?;

            if let Some(conn_pool) = &mut self.conn_pool{
                conn_pool.return_pool(new_conn).await?;
            }
            self.conn_info = None;
            self.conn_state = false;
            self.conn_pool = None;
        }
        Ok(())
    }

    /// mysql发生异常，关闭连接
    pub async fn reset_connection(&mut self) -> Result<()>{
        self.conn_info = None;
        self.conn_state = false;
        self.conn_pool = None;
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