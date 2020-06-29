/*
@author: xiao cai niao
@datetime: 2020/5/30
*/

use tokio::sync::Mutex;
use tokio::sync::{broadcast, mpsc, Semaphore};
use crate::mysql::connection::{MysqlConnection, PacketHeader};
use std::collections::{VecDeque, HashMap};
use crate::{Config, readvalue};
use crate::{Result, MyError};
use std::net::TcpStream;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::time::{SystemTime, Duration};
use std::error::Error;
use std::sync::{Arc, Condvar};
use std::{thread, time};
use crate::dbengine::client::ClientResponse;
use std::io::{Write, Read};
use tracing::field::{debug};
use std::ops::DerefMut;
use crate::mysql::connection::response::pack_header;
use tracing::{debug, error, info, instrument};
use std::future::Future;
use crate::server::shutdown::Shutdown;
use chrono::prelude::*;
use chrono;
use tokio::time::delay_for;

/// 连接池管理
///
/// conn_queue存放所有未缓存的空闲连接
///
/// cached_queue存放被连接缓存的mysql连接， 该缓存是由于事务未完成，所以放到此处不让别的连接使用，
/// 如果事务完成会放回conn_queue队列中。
///
/// queued_count 加 cached_count 加 active_count 是当前连接池中所有的连接数
#[derive(Debug, Clone)]
pub struct ConnectionsPool {
    conn_queue: Arc<Mutex<ConnectionInfo>>,                 //所有连接队列
    cached_queue: Arc<(Mutex<HashMap<String, MysqlConnectionInfo>>)>,       //已缓存的连接
    cached_count: Arc<AtomicUsize>,                              //当前缓存的连接数
    queued_count: Arc<AtomicUsize>,                              //当前连接池总连接数
    active_count: Arc<AtomicUsize>,                              //当前活跃连接数
    min_thread_count: Arc<AtomicUsize>,                          //代表连接池最小线程
    max_thread_count: Arc<AtomicUsize>,                          //最大线连接数
    panic_count: Arc<AtomicUsize>,                               //记录发生panic连接的数量
}

impl ConnectionsPool{
    pub fn new(conf: &Config) -> Result<ConnectionsPool>{
        let queue = ConnectionInfo::new(conf)?;
        let queued_count = Arc::new(AtomicUsize::new(queue.pool.len()));
        let conn_queue = Arc::new(Mutex::new(queue));
        Ok(ConnectionsPool{
            conn_queue,
            cached_queue: Arc::new(Mutex::new(HashMap::new())),
            cached_count: Arc::new(AtomicUsize::new(0)),
            queued_count,
            active_count: Arc::new(AtomicUsize::new(0)),
            min_thread_count: Arc::new(AtomicUsize::new(conf.min)),
            max_thread_count: Arc::new(AtomicUsize::new(conf.max)),
            panic_count: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// 从空闲队列中获取连接
    ///
    /// 获取时分为几种状态：
    ///
    /// 队列有空闲连接： 直接获取
    ///
    /// 队列没有空闲连接则判断是否已达到最大值：
    ///
    /// 如果没有： 创建连接并获取
    ///
    /// 如果已达到最大： 等待1秒，如果超过1秒没获取到则返回错误
    pub async fn get_pool(&mut self, key: &String) -> Result<MysqlConnectionInfo> {
        let std_duration = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
        let start_time = Duration::from(std_duration);
        let wait_time = Duration::from_millis(1000);    //等待时间1s
        match self.check_cache(key).await?{
            Some(conn) => {
                return Ok(conn);
            }
            None => {
                let mut pool = self.conn_queue.lock().await;
                loop {
                    //从队列中获取，如果队列为空，则判断是否已达到最大，再进行判断获取
                    if let Some(conn) = pool.pool.pop_front(){
                        drop(pool);
                        self.queued_count.fetch_sub(1, Ordering::SeqCst);
                        self.active_count.fetch_add(1, Ordering::SeqCst);
                        return Ok(conn);
                    }else {
                        let count = self.queued_count.load(Ordering::Relaxed) +
                            self.cached_count.load(Ordering::Relaxed) +
                            self.active_count.load(Ordering::Relaxed);

                        if &count < &self.max_thread_count.load(Ordering::Relaxed) {
                            //如果还未达到最大连接数则新建
                            pool.new_conn()?;
                            self.queued_count.fetch_add(1, Ordering::SeqCst);
                        }else {
                            drop(pool);
                            //已达到最大连接数则等待指定时间,看能否获取到
                            let now_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
                            pool  = if now_time - start_time > wait_time {
                                return Err(Box::new(MyError(String::from("获取连接超时").into())));
                            } else {
                                delay_for(Duration::from_millis(50)).await;
                                self.conn_queue.lock().await
                                //condvar.wait_timeout(pool, wait_time)?.0
                            };
                        }
                    }
                }
            }
        }
    }


    /// 操作完成归还连接到连接池中
    pub async fn return_pool(&mut self, conn: MysqlConnectionInfo) -> Result<()> {
        if conn.cached == String::from(""){
            //let &(ref pool, ref condvar) = &*self.conn_queue;
            let mut pool = self.conn_queue.lock().await;
            self.queued_count.fetch_add(1, Ordering::SeqCst);
            self.active_count.fetch_sub(1, Ordering::SeqCst);
            pool.pool.push_back(conn);
        }else {
            let mut cache_pool = self.cached_queue.lock().await;
            cache_pool.insert(conn.cached.clone(), conn);
            self.cached_count.fetch_add(1, Ordering::SeqCst);
            self.active_count.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(())
    }


    /// 检查是否有已指定的连接
    ///
    /// 如果有返回对应连接信息，如果没有则返回None
    async fn check_cache(&mut self, key: &String) -> Result<Option<MysqlConnectionInfo>>{
        let mut cache_pool = self.cached_queue.lock().await;
        match cache_pool.remove(key){
            Some(v) => {
                self.cached_count.fetch_sub(1, Ordering::SeqCst);
                self.active_count.fetch_add(1, Ordering::SeqCst);
                return Ok(Some(v));
            },
            None => {
                return Ok(None);
            }
        }


    }


    /// 连接池中连接的维护
    ///
    /// 如果有断开的连接会自动补齐满足随时都有最小连接数
    ///
    /// 对空闲连接进行心跳检测
    ///
    /// 对缓存连接进行空闲时间检测， 超过时间放回连接池
    pub async fn check_health(&mut self) -> Result<()>{
        let mut ping_last_check_time = Local::now().timestamp_millis() as usize;
        let mut maintain_last_check_time = Local::now().timestamp_millis() as usize;
        'a: loop {
            let now_time = Local::now().timestamp_millis() as usize;
            //每隔60秒检查一次
            if now_time - ping_last_check_time >= 60000{
                info!("{}", String::from("check_ping"));
                self.check_ping().await?;
                ping_last_check_time = now_time;
            }
            //每隔600秒进行一次连接池数量维护
            if now_time - maintain_last_check_time >= 600000{
                info!("{}", String::from("maintain_pool"));
                self.maintain_pool().await?;
                maintain_last_check_time = now_time;
            }

            //self.maintain_cache_pool().await?;
            delay_for(Duration::from_millis(50)).await;
        }
    }

    /// 维护线程池中线程的数量， 如果低于最小值补齐
    ///
    /// 如果大于最小值，则进行空闲时间检测，空闲时间超过600s的将断开
    pub async fn maintain_pool(&mut self) -> Result<()> {
        let count = self.active_count.load(Ordering::Relaxed) + self.queued_count.load(Ordering::Relaxed) + self.cached_count.load(Ordering::Relaxed);
        let min = self.min_thread_count.load(Ordering::Relaxed);
        if &count < &min {
            let t = min - count;
            //let &(ref pool, ref condvar) = &*self.conn_queue;
            let mut pool = self.conn_queue.lock().await;
            for _i in 0..t{
                pool.new_conn()?;
                self.queued_count.fetch_add(1, Ordering::SeqCst);
            }
        }else if &count > &min {
            // 超过最小连接数， 对连接进行空闲检查，达到阈值时断开连接
            // 最低只会减少到最小连接数
            let mut pool = self.conn_queue.lock().await;
            let num = (count - min) as u32;
            let mut tmp = 0 as u32;
            for _ in 0..pool.pool.len(){
                if let Some(mut conn) = pool.pool.pop_front(){
                    self.queued_count.fetch_sub(1, Ordering::SeqCst);
                    self.active_count.fetch_add(1, Ordering::SeqCst);
                    if !conn.check_sleep(){
                        pool.pool.push_back(conn);
                        self.queued_count.fetch_add(1, Ordering::SeqCst);
                        self.active_count.fetch_sub(1, Ordering::SeqCst);
                    }else {
                        conn.close();
                        tmp += 1;
                        if tmp >= num{
                            break;
                        }
                    }
                }
            }
            drop(pool);
        }
        Ok(())
    }

    /// 对缓存连接池进行维护, 空闲超过阈值且不存在事务的则直接放回连接池，供其他连接使用
    pub async fn maintain_cache_pool(&mut self) -> Result<()> {
        if self.cached_count.load(Ordering::Relaxed) > 0 {
            let mut cache_pool = self.cached_queue.lock().await;
            let mut tmp: Vec<String> = vec![];
            for (key, conn) in cache_pool.iter_mut(){

                if conn.check_cacke_sleep(){
                    tmp.push(key.clone());
                }
            }
            for key in tmp {
                let mut pool = self.conn_queue.lock().await;
                match cache_pool.remove(&key){
                    Some(mut conn) => {
                        conn.reset_cached().await?;
                        conn.reset_conn_default()?;
                        pool.pool.push_back(conn);
                        self.queued_count.fetch_add(1, Ordering::SeqCst);
                        self.cached_count.fetch_sub(1, Ordering::SeqCst);
                    }
                    None => {}
                }
            }
            drop(cache_pool)
        }
        Ok(())
    }

    /// 对连接池中的连接进行心跳检查
    pub async fn check_ping(&mut self) -> Result<()> {
        //检查空闲连接
        if self.queued_count.load(Ordering::Relaxed) > 0 {
            let mut pool = self.conn_queue.lock().await;
            for _ in 0..pool.pool.len() {
                if let Some(mut conn) = pool.pool.pop_front(){
                    self.queued_count.fetch_sub(1, Ordering::SeqCst);
                    self.active_count.fetch_add(1, Ordering::SeqCst);
                    match conn.check_health(){
                        Ok(b) =>{
                            if b{
                                pool.pool.push_back(conn);
                                self.queued_count.fetch_add(1, Ordering::SeqCst);
                                self.active_count.fetch_sub(1, Ordering::SeqCst);
                            }else {
                                error!("{}",String::from("check ping failed"));
                            }
                        }
                        Err(e) => {
                            error!("{}",format!("check ping error: {:?}", e.to_string()));
                        }
                    }
                }
            }
            drop(pool);
        }
        Ok(())
    }

}

#[derive(Debug)]
pub struct MysqlConnectionInfo {
    pub conn: TcpStream,
    pub cached: String,             //记录使用该连接的线程hash，用于不自动提交的update/insert/delete
    pub last_time: usize,          //记录该mysql连接最后执行命令的时间，用于计算空闲时间，如果没有设置缓存标签在达到200ms空闲时将放回连接池
    pub is_transaction: bool,        //记录是否还有事务存在
    pub is_write: bool,            //是否为写入连接
}
impl MysqlConnectionInfo{
    pub fn new(conn: MysqlConnection) -> MysqlConnectionInfo{
        MysqlConnectionInfo{
            conn: conn.conn,
            cached: "".to_string(),
            last_time: 0,
            is_transaction: false,
            is_write: false
        }
    }

    pub fn try_clone(&self) -> Result<MysqlConnectionInfo>{
        Ok(MysqlConnectionInfo{
            conn: self.conn.try_clone()?,
            cached: self.cached.clone(),
            last_time: self.last_time.clone(),
            is_transaction: self.is_transaction.clone(),
            is_write: self.is_write.clone()
        })
    }

    pub fn set_last_time(&mut self) {
        let dt = Local::now();
        let last_time = dt.timestamp_millis() as usize;
        self.last_time = last_time;
    }

    /// 检查空闲时间，超过200ms返回true
    pub fn check_cacke_sleep(&mut self) -> bool {
        let dt = Local::now();
        let now_time = dt.timestamp_millis() as usize;
        if now_time - self.last_time > 200 && !self.is_transaction {
            return true
        }
        false
    }

    /// 检查空闲时间，超过600s返回true
    pub fn check_sleep(&mut self) -> bool {
        let dt = Local::now();
        let now_time = dt.timestamp_millis() as usize;
        if now_time - self.last_time > 600000 {
            return true
        }
        false
    }

    pub fn hash_cache(&self, key: &String) -> bool{
        if &self.cached == key {
            true
        }else {
            false
        }
    }

    pub async fn set_is_transaction(&mut self) -> Result<()>{
        self.is_transaction = true;
        Ok(())
    }

    pub async fn reset_is_transaction(&mut self) -> Result<()>{
        self.is_transaction = false;
        Ok(())
    }

    pub async fn set_cached(&mut self, key: &String) -> Result<()> {
        if key != &self.cached{
            self.cached = key.clone();
        }
        Ok(())
    }

    pub async fn reset_cached(&mut self) -> Result<()>{
        self.cached = "".to_string();
        Ok(())
    }

    /// send packet and return response packet
    pub fn send_packet(&mut self, packet: &Vec<u8>) -> Result<(Vec<u8>, PacketHeader)> {
        self.conn.write_all(packet)?;
        let (buf, header) = self.get_packet_from_stream()?;
        self.set_last_time();
        Ok((buf, header))
    }

    /// send packet and return response packet for sync
    fn __send_packet(&mut self, packet: &Vec<u8>) -> Result<(Vec<u8>, PacketHeader)> {
        self.conn.write_all(packet)?;
        let (buf, header) = self.__get_packet_from_stream()?;
        Ok((buf, header))
    }

    /// read packet from socket
    ///
    /// if payload = 0xffffff： this packet more than the 64MB
    fn __get_packet_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let (mut buf,header) = self.__get_from_stream()?;
        while header.payload == 0xffffff{
            debug(header.payload);
            let (buf_tmp,_) = self.__get_from_stream()?;
            buf.extend(buf_tmp);
        }
        Ok((buf, header))
    }

    /// read on packet from socket
    fn __get_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let mut header_buf = vec![0 as u8; 4];
        let mut header: PacketHeader = PacketHeader { payload: 0, seq_id: 0 };
        loop {
            match self.conn.read_exact(&mut header_buf){
                Ok(_) => {
                    header = PacketHeader::new(&header_buf)?;
                    if header.payload > 0 {
                        break;
                    }
                }
                Err(e) => {
                    debug(e);
                }
            }

        }

        //read the actual data through the payload data obtained through the packet header
        let mut packet_buf  = vec![0 as u8; header.payload as usize];
        match self.conn.read_exact(&mut packet_buf) {
            Ok(_) =>{}
            Err(e) => {
                debug(format!("read packet error:{}",e));
            }
        }
        return Ok((packet_buf,header));
    }

    /// read packet from socket
    ///
    /// if payload = 0xffffff： this packet more than the 64MB
    pub fn get_packet_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let (mut buf,header) = self.get_from_stream()?;
        while header.payload == 0xffffff{
            debug(header.payload);
            let (buf_tmp,_) = self.get_from_stream()?;
            buf.extend(buf_tmp);
        }
        Ok((buf, header))
    }

    /// read on packet from socket
    fn get_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let mut header_buf = vec![0 as u8; 4];
        let mut header: PacketHeader = PacketHeader { payload: 0, seq_id: 0 };
        loop {
            match self.conn.read_exact(&mut header_buf){
                Ok(_) => {
                    header = PacketHeader::new(&header_buf)?;
                    if header.payload > 0 {
                        break;
                    }
                }
                Err(e) => {
                    debug(e);
                }
            }

        }

        //read the actual data through the payload data obtained through the packet header
        let mut packet_buf  = vec![0 as u8; header.payload as usize];
        match self.conn.read_exact(&mut packet_buf) {
            Ok(_) =>{}
            Err(e) => {
                debug(format!("read packet error:{}",e));
            }
        }
        return Ok((packet_buf,header));
    }

    fn set_autocommit(&mut self) -> Result<()> {
        let sql = format!("set autocommit=0;");
        let packet_full = self.set_default_packet(&sql);
        let (a, b) = self.__send_packet(&packet_full)?;
        self.check_packet_is(&a)?;
        Ok(())
    }

    fn set_default_packet(&mut self, sql: &String) -> Vec<u8> {
        let mut packet = vec![];
        packet.push(3 as u8);
        packet.extend(sql.as_bytes());
        let mut packet_full = vec![];
        packet_full.extend(pack_header(&packet, 0));
        packet_full.extend(packet);
        return packet_full;
    }

    pub fn check_packet_is(&self, buf: &Vec<u8>) -> Result<()>{
        if buf[0] == 0xff {
            let error = readvalue::read_string_value(&buf[3..]);
            return Err(Box::new(MyError(error.into())));
        }
        Ok(())
    }

    /// ping 检查连接健康状态
    pub fn check_health(&mut self) -> Result<bool> {
        let mut packet: Vec<u8> = vec![];
        packet.extend(readvalue::write_u24(1));
        packet.push(0);
        packet.push(0x0e);
        if let Err(e) = self.conn.write_all(&packet){
            debug(e.to_string());
            return Ok(false);
        };
        info!("{}", String::from("oook"));
        let (buf, header) = self.get_packet_from_stream()?;
        if let Err(e) = self.check_packet_is(&buf){
            debug(e.to_string());
            self.close();
            return Ok(false);
        }

        return Ok(true);

    }

    /// 初始化连接为默认状态
    ///
    /// 用于缓存线程归还到线程池时使用
    pub fn reset_conn_default(&mut self) -> Result<()>{
        self.set_autocommit()?;
        let sql = String::from("use information_schema");
        let packet_full = self.set_default_packet(&sql);
        let (a, b) = self.__send_packet(&packet_full)?;
        self.check_packet_is(&a)?;
        Ok(())

    }

    pub fn close(&mut self) {
        let mut packet: Vec<u8> = vec![];
        packet.extend(readvalue::write_u24(1));
        packet.push(0);
        packet.push(1);
        if let Err(_e) = self.conn.write_all(&packet){}
    }
}


#[derive(Debug)]
struct ConnectionInfo {
    pool: VecDeque<MysqlConnectionInfo>,                        //mysql连接队列
    config: Config
}

impl ConnectionInfo {
    fn new(conf: &Config) -> Result<ConnectionInfo> {
        if conf.min > conf.max || conf.max == 0 {
            return Err(Box::new(MyError(String::from("mysql连接池min/max配置错误").into())));
        }
        let mut pool = ConnectionInfo {
            pool: VecDeque::with_capacity(conf.max),
            config: conf.clone()
        };
        for _ in 0..conf.min {
            pool.new_conn()?;
        }

        Ok(pool)
    }
    fn new_conn(&mut self) -> Result<()> {
        match MysqlConnection::new(&self.config) {
            Ok(mut conn) => {
                conn.create(&self.config)?;
                let mut conn_info = MysqlConnectionInfo::new(conn);
                conn_info.set_autocommit()?;
                self.pool.push_back(conn_info);
                Ok(())
            }
            Err(err) => Err(err)
        }
    }

}





