use crate::mysql::pool::{MysqlConnectionInfo, ConnectionsPoolPlatform, ConnectionsPool};
use std::time::Duration;
use crate::mysql::Result;
use tokio::time::delay_for;
use crate::server::sql_parser::SqlStatement;
use crate::MyError;
use tracing::{debug, error,info};
use crate::server::ClassTime;
use chrono::Local;

/// mysql connection
#[derive(Debug)]
pub struct PerMysqlConn{
    pub conn_info: Option<MysqlConnectionInfo>,
    pub conn_pool: Option<ConnectionsPool>,
    pub conn_state: bool,
    pub cur_db: String,
    pub cur_autocommit: bool,
    pub platform_is_sublist: bool,
    pub platform: String,
}

impl PerMysqlConn {
    pub fn new() -> PerMysqlConn {
        PerMysqlConn{ conn_info: None, conn_pool: None, conn_state: false,
            cur_db: "information_schema".to_string(), cur_autocommit: false,
            platform_is_sublist: false, platform: "".to_string() }
    }

    /// 检查连接空闲状态， 空闲超过指定时间且无事务操作则归还连接到连接池
    pub async fn health(&mut self) -> Result<()> {
        loop {
            if let Some(conn) = &mut self.conn_info{
                if conn.check_cacke_sleep(){
                    if let Err(e) = conn.reset_conn_default(){
                        //操作连接异常，只重置状态，不归还连接
                        error!("reset conn default err(per_connnection_health): {}", e.to_string());
                        self.cancel_failed_connection().await;
                        continue;
                    }

                    //操作正常则进行下一步归还连接
                    match conn.try_clone(){
                        Ok(new_conn) => {
                            self.return_connection_pool(new_conn).await?;
                        }
                        Err(e) => {
                            //操作连接异常，只重置状态，不归还连接
                            error!("trye clone err(per_connnection_health): {}", e.to_string());
                            self.cancel_failed_connection().await;
                        }
                    }
                    break;
                }
            }
            delay_for(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    /// 异常连接直接丢弃，并减少连接池活跃连接计数值
    pub async fn cancel_failed_connection(&mut self)  {
        self.reset_my_state().await;
        if let Some(conn_pool) = &mut self.conn_pool{
            conn_pool.sub_active_count().await;
            conn_pool.return_platform_conn_count(&self.platform).await;
        }
    }

    /// 客户端连接非正常结束导致的异常关闭， 归还连接流程
    ///
    /// 首先设置当前连接的cached为空，再执行return连接，这里避免归还的连接为prepare语句的
    /// 如果不修改cache值，客户端连接最终退出会造成cache_queque中会一直存在一个僵尸连接
    ///
    /// 最后再从cached_queue中获取是否有缓存的连接， 如果有再归还到可用队列中，
    /// 这里是为了避免连接处在cached_queue中， 而连接又退出，会产生无法正常归还的情况
    pub async fn cancel_error_return_connection(&mut self, seq: u8, pool: &mut ConnectionsPoolPlatform, key: &String) -> Result<()>{
        self.reset_cached().await?;
        self.return_connection(seq).await?;

        self.reset_my_state().await;
        if let (Some(mut conn_info), Some(mut conn_pool)) = pool.get_cached_conn(key).await?{
            conn_info.reset_cached().await?;
            conn_pool.return_pool(conn_info, &self.platform).await?;
        }
        Ok(())
    }


    /// 重置当前连接cached值
    pub async fn reset_cached(&mut self) -> Result<()> {
        if let Some(conn_info) = &mut self.conn_info{
            return Ok(conn_info.reset_cached().await?);
        }
        Ok(())
    }

    async fn reset_my_state(&mut self) {
        self.conn_info = None;
        self.conn_state = false;
        self.cur_db = "information_schema".to_string();
        self.cur_autocommit = false;
    }

    pub async fn check(&mut self,  pool: &mut ConnectionsPoolPlatform, key: &String,
                       db: &Option<String>, auto_commit: &bool,
                       sql_type: &SqlStatement,seq: u8, select_comment: Option<String>, platform: &String) -> Result<()> {
        if !self.conn_state{
            self.check_get(pool, key, db, auto_commit, sql_type, &select_comment,platform).await?;
        }else {
            if let Some(conn) = &self.conn_info{
                //检查当前语句是否使用当前连接
                if pool.conn_type_check(&conn.host_info, sql_type, &select_comment,key).await?{
                    self.check_default_db_and_autocommit(db, auto_commit).await?;
                }else {
                    //不能使用，则需要重新获取连接， 先归还当前连接到连接池
                    self.return_connection(seq).await?;
                    self.check_get(pool, key, db, auto_commit, sql_type, &select_comment, platform).await?;
                }
            }
        }
        Ok(())
    }

    async fn check_get(&mut self, pool: &mut ConnectionsPoolPlatform, key: &String, db: &Option<String>,
                       auto_commit: &bool, sql_type: &SqlStatement, select_comment: &Option<String>, platform: &String) -> Result<(Vec<ClassTime>)>{
        let mut class_times = vec![ClassTime::new(String::from("per_connections check_get"))];
        debug!("get connection from thread_pool");
        let (conn, conn_pool) = pool.get_pool(sql_type,key, select_comment,
                                              platform, self.platform_is_sublist.clone()).await?;
        class_times.push(ClassTime::new(String::from("get_pool ok")));
        debug!("OK");
        self.conn_info = Some(conn);
        self.conn_pool = Some(conn_pool);
        class_times.push(ClassTime::new(String::from("set_default_info")));
        // info!("db: {:?}", db);
        if let Err(e) = self.set_default_info(db, auto_commit).await{
            error!("set default info error: {}", e.to_string());
        }
        class_times.push(ClassTime::new(String::from("set_default_info ok")));
        self.conn_state = true;
        Ok((class_times))
    }

    /// 判断是否开启审计，开启则打印sql
    pub async fn check_auth_save(&mut self, sql: &String, host: &String) {
        if let Some(conn_pool) = &self.conn_pool{
            conn_pool.auth_save(sql, host).await;
        }
    }

    /// 检查是否存在未提交事务
    pub async fn check_have_transaction(&self) -> Result<()>{
        debug!("check for uncommitted transactions");
        match &self.conn_info {
            Some(conn) =>{
                if conn.is_transaction{
                    let err = String::from("must commit outstanding transactions");
                    error!("{}", err);
                    return Err(Box::new(MyError(err.into())));
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn set_default_info(&mut self, db: &Option<String>, auto_commit: &bool) -> Result<()> {
        debug!("set default information fo connection");
        match &mut self.conn_info {
            Some(conn) => {
                if !conn.check_health().await?{
                    return Err(Box::new(MyError(String::from("lost connection for mysql"))));
                }
                match db{
                    Some(v) => {
                        conn.set_default_db(v.clone())?;
                        self.cur_db = v.clone();
                    }
                    None => {
                        info!("db:{:?}", db);
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

    /// 归还连接
    pub async fn return_connection(&mut self, seq: u8) -> Result<()> {
        if let Some(conn) = &mut self.conn_info{
            match conn.try_clone(){
                Ok(mut new_conn) => {
                    // 如果有事务存在则回滚， 回滚失败会结束该连接
                    new_conn.check_rollback(seq).await?;
                    // 归还连接
                    self.return_connection_pool(new_conn).await?;
                }
                Err(e) => {
                    error!("try clone error: {:?}", e.to_string());
                    // try clone发生错误， 直接丢弃连接并修改连接统计
                    self.cancel_failed_connection().await;
                }
            }
        }
        Ok(())
    }

    /// 归还连接到对应连接池
    async fn return_connection_pool(&mut self, new_conn: MysqlConnectionInfo) -> Result<()>{
        if let Some(conn_pool) = &mut self.conn_pool{
            conn_pool.return_pool(new_conn, &self.platform).await?;
        }
        // 重置连接状态
        self.reset_my_state().await;
        Ok(())
    }

    pub async fn get_connection_host_info(&self) -> String {
        let mut host_info = String::from("");
        if let Some(conn) = &self.conn_info{
            host_info = conn.host_info.clone();
        }
        host_info
    }

    fn call_times(&self) -> usize {
        let dt = Local::now();
        let cur_timestamp = dt.timestamp_millis() as usize;
        return cur_timestamp
    }

    /// 对当前连接进行ops计数
    pub async fn save_ops(&mut self, sql_type: &SqlStatement) {
        if let Some(node_pool) = &mut self.conn_pool{
            node_pool.save_ops_info(sql_type).await;
        }
    }
}