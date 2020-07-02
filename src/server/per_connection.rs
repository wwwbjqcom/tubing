use crate::mysql::pool::{MysqlConnectionInfo, ConnectionsPool};
use std::time::Duration;
use crate::Result;
use tokio::time::delay_for;
use tracing::{info};

/// mysql connection
#[derive(Debug)]
pub struct PerMysqlConn{
    pub conn_info: Option<MysqlConnectionInfo>,
    pub conn_state: bool
}

impl PerMysqlConn {
    pub fn new() -> PerMysqlConn {
        PerMysqlConn{ conn_info: None, conn_state: false }
    }

    /// 检查连接空闲状态， 空闲超过指定时间且无事务操作则归还连接到连接池
    pub async fn health(&mut self, pool: &mut ConnectionsPool) -> Result<()> {
        loop {
            if let Some(conn) = &mut self.conn_info{
                if conn.check_cacke_sleep(){
                    info!("abc");
                    conn.reset_cached().await?;
                    conn.reset_conn_default()?;
                    let mut new_conn = conn.try_clone()?;
                    pool.return_pool(new_conn).await?;
                    self.conn_info = None;
                    self.conn_state = false;
                    break;
                }
            }
            delay_for(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    pub async fn check(&mut self,  pool: &mut ConnectionsPool, key: &String) -> Result<()> {
        if !self.conn_state{
            let conn = pool.get_pool(key).await?;
            self.conn_info = Some(conn);
            self.set_default_info().await?;
            self.conn_state = true;
        }
        Ok(())
    }

    pub async fn set_default_info(&mut self) -> Result<()> {
        match &mut self.conn_info {
            Some(conn) => {
                conn.reset_conn_default()?;
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