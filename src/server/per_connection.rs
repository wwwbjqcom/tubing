use crate::mysql::pool::{MysqlConnectionInfo, ConnectionsPool};
use std::time::Duration;
use crate::Result;
use tokio::time::delay_for;
use tracing::{info, debug};

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
    pub async fn health(&mut self, pool: &mut ConnectionsPool) -> Result<()> {
        loop {
            if let Some(conn) = &mut self.conn_info{
                if conn.check_cacke_sleep(){
                    conn.reset_conn_default()?;
                    let mut new_conn = conn.try_clone()?;
                    pool.return_pool(new_conn).await?;
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

    pub async fn check(&mut self,  pool: &mut ConnectionsPool, key: &String, db: &Option<String>, auto_commit: &bool) -> Result<()> {
        if !self.conn_state{
            let conn = pool.get_pool(key).await?;
            self.conn_info = Some(conn);
            self.set_default_info(db, auto_commit).await?;
            self.conn_state = true;
        }else {
            self.check_default_db_and_autocommit(db, auto_commit).await;
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

    pub async fn return_connection(&mut self, pool: &mut ConnectionsPool) -> Result<()> {
        if let Some(conn) = &mut self.conn_info{
            conn.reset_cached().await?;
            conn.reset_conn_default()?;
            let mut new_conn = conn.try_clone()?;
            pool.return_pool(new_conn).await?;
            self.conn_info = None;
            self.conn_state = false;
        }
        Ok(())
    }

}