/*
@author: xiao cai niao
@datetime: 2020/5/30
*/

use tokio::sync::Mutex;
use crate::mysql::connection::{MysqlConnection, PacketHeader};
use std::collections::{VecDeque, HashMap};
use crate::{Config, readvalue};
use crate::Result;
use std::net::TcpStream;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::time::{SystemTime, Duration};
use std::error::Error;
use std::sync::{Arc, Condvar};
use std::thread;
use crate::dbengine::client::ClientResponse;
use std::io::{Write, Read};
use tracing::field::debug;
use std::ops::DerefMut;
use crate::mysql::connection::response::pack_header;

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
                                return Box::new(Err("获取连接超时")).unwrap();
                            } else {
                                thread::sleep(Duration::from_millis(1));
                                self.conn_queue.lock().await
                                //condvar.wait_timeout(pool, wait_time)?.0
                            };
                        }
                    }
                }
            }
        }
    }

//    fn __get(&mut self, mut pool: &MutexGuard<ConnectionInfo>) -> Result<MysqlConnectionInfo> {
//        //从队列中获取，如果队列为空，则判断是否已达到最大，再进行判断获取
//        let std_duration = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
//        let start_time = Duration::from(std_duration);
//        let wait_time = Duration::from_millis(1000);    //等待时间1s
//        loop {
//            //从队列中获取，如果队列为空，则判断是否已达到最大，再进行判断获取
//            if let Some(conn) = pool.pool.pop_front(){
//                if conn.cached == String::from(""){
//                    drop(pool);
//                    self.active_count.fetch_add(1, Ordering::SeqCst);
//                    return conn;
//                }else {
//                    self.return_pool(conn)?;
//                    continue;
//                }
//            }else {
//                if self.queued_count.load(Ordering::Relaxed) < self.max_thread_count.load(Ordering::Relaxed) {
//                    //如果还未达到最大连接数则新建
//                    pool.new_conn()?;
//                    self.queued_count.fetch_add(1, Ordering::SeqCst);
//                }else {
//                    //已达到最大连接数则等待指定时间,看能否获取到
//                    let now_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
//                    pool  = if now_time - start_time > wait_time {
//                        return Err("获取连接超时");
//                    } else {
//                        condvar.wait_timeout(pool, wait_time)?.0
//                    };
//                }
//            }
//        }
//
//    }

    /// 操作完成归还连接到连接池中
    pub async fn return_pool(&mut self, conn: MysqlConnectionInfo) -> Result<()> {
        if conn.cached == String::from(""){
            //let &(ref pool, ref condvar) = &*self.conn_queue;
            let mut pool = self.conn_queue.lock().await;
            self.active_count.fetch_add(1, Ordering::SeqCst);
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
            None => {Ok(None)}
        }

//        let mut id = None;
//        let &(ref pool, ref condvar) = &*self.conn_queue;
//        let pool = pool.lock()?;
//        for (i, conn) in pool.pool.iter().rev().enumerate() {
//            if conn.hash_cache(key) {
//                id = Some(i);
//                break;
//            }
//        }
//        match id {
//            Some(id) => Ok(id.and_then(|id| pool.pool.swap_remove_back(id))),
//            None => None
//        }

    }

    /// 连接池中连接的维护
    ///
    /// 如果有断开的连接会自动补齐满足随时都有最小连接数
    pub async fn check_health(&mut self) -> Result<()>{
        let active_count = self.active_count.load(Ordering::Relaxed);
        let min = self.min_thread_count.load(Ordering::Relaxed);
        if &active_count < &min {
            let t = active_count - min;
            //let &(ref pool, ref condvar) = &*self.conn_queue;
            let mut pool = self.conn_queue.lock().await;
            for _i in 0..t{
                pool.new_conn()?;
            }
        }
        Ok(())
    }

}

#[derive(Debug)]
pub struct MysqlConnectionInfo {
    pub conn: TcpStream,
    pub cached: String,             //记录使用该连接的线程hash，用于不自动提交的update/insert/delete
}
impl MysqlConnectionInfo{
    pub fn new(conn: MysqlConnection) -> MysqlConnectionInfo{
        MysqlConnectionInfo{
            conn: conn.conn,
            cached: "".to_string()
        }
    }

    pub fn hash_cache(&self, key: &String) -> bool{
        if &self.cached == key {
            true
        }else {
            false
        }
    }

    pub async fn set_cached(&mut self, key: &String) -> Result<()> {
        self.cached = key.clone();
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
        let mut packet = vec![];
        packet.push(3 as u8);
        let sql = format!("set autocommit=0;");
        packet.extend(sql.as_bytes());
        let mut packet_full = vec![];
        packet_full.extend(pack_header(&packet, 0));
        packet_full.extend(packet);
        let (a, b) = self.__send_packet(&packet_full)?;
        self.check_packet_is(&a)?;
        Ok(())
    }

    pub fn check_packet_is(&self, buf: &Vec<u8>) -> Result<()>{
        if buf[0] == 0xff {
            let error = readvalue::read_string_value(&buf[3..]);
            return Box::new(Err(error)).unwrap();
        }
        Ok(())
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
            return Box::new(Err(String::from("mysql连接池min/max配置错误"))).unwrap();
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




