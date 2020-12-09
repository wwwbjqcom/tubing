/*
@author: xiao cai niao
@datetime: 2020/5/30
*/


/*
记录所有连接池相关操作， 包含：
1、各业务后端数据库集群连接池管理
2、连接池使用及归还
3、读写路由
4、维护连接池状态
5、节点变动动态修改对应连接池清空
6、记录各节点、业务的ops状态
*/

use tokio::sync::{Mutex, RwLock};
use crate::mysql::connection::{MysqlConnection, PacketHeader, AllUserInfo};
use std::collections::{VecDeque, HashMap};
use crate::{Config, readvalue, Platform, MyConfig};
use crate::{MyError};
use crate::mysql::Result;
use crate::server::mysql_mp;
use std::net::TcpStream;
use std::sync::atomic::{Ordering, AtomicUsize, AtomicBool};
use std::time::{SystemTime, Duration};
use std::sync::{Arc};
use std::io::{Write, Read};
use tracing::field::{debug};
use crate::mysql::connection::response::pack_header;
use tracing::{debug, error, info};
use chrono::prelude::*;
use chrono;
use tokio::time::delay_for;
use crate::server::sql_parser::SqlStatement;
use crate::server::mysql_mp::RouteInfo;
use crate::dbengine::admin;
use crate::mysql::privileges::MetaColumn;


// #[derive(Debug)]
// pub struct PlatformAuth{
//     platform: String,
//     auth: Arc<AtomicBool>
// }
// impl PlatformAuth{
//     fn new(platform: &Platform) -> PlatformAuth{
//         PlatformAuth{
//             platform: platform.platform.clone(),
//             auth: Arc::new(AtomicBool::new(platform.auth.clone()))
//         }
//     }
// }
//
// #[derive(Debug)]
// pub struct AllPlatformAuth{
//     all_platform: Arc<HashMap<String, PlatformAuth>>,
//     config: MyConfig
// }
//
// impl AllPlatformAuth{
//     pub fn new(conf: &MyConfig) -> AllPlatformAuth{
//         let mut all_platform = HashMap::new();
//         for platform in &conf.platform{
//             let platform_auth = PlatformAuth::new(platform);
//             all_plaform.insert(platform.platform.clone(), platform_auth);
//         }
//         AllPlatformAuth{ all_platform: Arc::new(all_platform), config: conf.clone() }
//     }
//
//     pub async fn check_auth(&self, client_platform: &String) -> bool{
//         if let Some(platform_name) = self.config.get_pool_platform(client_platform){
//             if let Some(platform_auth) = self.all_platform.get(&platform_name){
//                 if platform_auth.auth.load(Ordering::Relaxed){
//                     return true
//                 }
//             }
//         }
//         return false;
//     }
//
//     pub async fn alter_auth(&self, platform: &String, auth: bool){
//         if let Some(platform_auth) = self.all_platform.get(&platform_name){
//             if platform_auth.auth.store(auth){
//                 return true
//             }
//         }
//     }
// }


enum HealthType{
    Ping,
    Maintain
}

#[derive(Clone, Debug)]
pub struct PlatforNodeInfo{
    pub platform: String,
    pub mgr: bool,
    pub write: String,
    pub read: Vec<String>,
    pub read_is_alter: bool,
    pub write_is_alter: bool
}
impl PlatforNodeInfo{
    fn new(platform: &Platform) -> PlatforNodeInfo{
        let mut read = vec![];
        if let Some(v) = &platform.read{
            read = v.clone();
        }
        read.push(platform.get_write_host());
        read.sort_by(|a,b|a.to_lowercase().cmp(&b.to_lowercase()));
        let mut mgr = false;
        if let Some(v) = platform.mgr{
            mgr = v;
        }
        PlatforNodeInfo{
            platform: platform.platform.clone(),
            mgr,
            write: platform.get_write_host(),
            read,
            read_is_alter: false,
            write_is_alter: false
        }
    }

    fn reset_alter(&mut self) {
        self.read_is_alter = false;
        self.write_is_alter = false;
    }

    /// 检查路由是否变动，如果变动则更改且返回true
    fn check(&mut self, route_info: &RouteInfo) -> bool {
        let write = route_info.get_write_host_info();
        let mut read = vec![];
        if let Some(v) = route_info.get_read_host_info(1){
            read = v;
        }
        debug!("route info check: {:?}, route_info_read:{:?}, read:{:?}", &self.write,&read, &self.read);
        if write == self.write && self.check_read_list(&read){
            return false;
        }
        if write != self.write{
            debug!("write no");
            self.write_is_alter = true;
            self.write = write.clone();
        }
        if !self.check_read_list(&read){
            debug!("read no");
            self.read_is_alter = true;
            self.read = read.clone();
        }
        debug!("{:?}", self);
        return true;
    }

    /// 检查读列表是否相等
    fn check_read_list(&mut self, read: &Vec<String>) -> bool{
        return if self.read.len() == read.len() {
            for i in 0..self.read.len() {
                if &self.read[i] != &read[i] {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }
}

/// 所有业务平台连接池集合以及用户列表
#[derive(Clone, Debug)]
pub struct PlatformPool{
    pub platform_pool: Arc<Mutex<HashMap<String, ConnectionsPoolPlatform>>>,         //存储所有业务平台的连接池， 以platform做为key
    pub user_info: Arc<RwLock<AllUserInfo>>,
    pub platform_node_info: Vec<PlatforNodeInfo>,                                           //记录每个业务平台后端数据库读写关系,变更时同时变更连接池
    pub config: MyConfig
}

impl PlatformPool{
    pub fn new(conf: &MyConfig) -> Result<(PlatformPool,AllUserInfo)>{
        let all_user_info = AllUserInfo::new(conf);
        debug!("{:?}", &all_user_info);
        let user_info = Arc::new(RwLock::new(all_user_info.clone()));
        let mut pool = HashMap::new();
        let mut platform_node_info = vec![];
        for platform in &conf.platform{
            debug!("create pool for {}", &platform.platform);
            let platform_pool = ConnectionsPoolPlatform::new(platform)?;
            pool.insert(platform.platform.clone(), platform_pool);
            platform_node_info.push(PlatforNodeInfo::new(platform));
        }
        let platform_pool = Arc::new(Mutex::new(pool));
        Ok((
            PlatformPool{
                platform_pool,
                user_info,
                platform_node_info,
                config: conf.clone()
            }, all_user_info)
        )
    }

    /// 获取对应业务库总连接池、当前platform是否为子业务
    pub async fn get_platform_pool(&mut self, platform: &String) -> (Option<ConnectionsPoolPlatform>, bool){
        if let  (Some(client_platform), is_sub) = self.config.get_pool_platform(platform){
            let platform_pool_lock = self.platform_pool.lock().await;
            if let Some(on_platform_pool) = platform_pool_lock.get(&client_platform){
                return (Some(on_platform_pool.clone()), is_sub)
            }
        }
        return (None, false)
    }

    /// 当连接执行set platform时, 通过保存的用户信息进行判断权限
    pub async fn check_conn_privileges(&self, platform: &String, user_name: &String) -> bool {
        let user_info_lock = self.user_info.read().await;
        return user_info_lock.check_platform_privileges(platform, user_name);
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
        let mut route_last_check_time = Local::now().timestamp_millis() as usize;
        loop {
            let now_time = Local::now().timestamp_millis() as usize;
            //每隔120秒进行一次连接池数量维护
            if now_time - maintain_last_check_time >= 120000{
                debug!("{}", String::from("maintain_pool"));
                if let Err(e) = self.check_health_for_type(HealthType::Maintain).await{
                    error!("pool maintain error:{}", e.to_string());
                }
                maintain_last_check_time = now_time;
            }

            //每隔600秒心跳检查一次
            if now_time - ping_last_check_time >= 600000{
                debug!("{}", String::from("check_ping"));
                if let Err(e) = self.check_health_for_type(HealthType::Ping).await{
                    error!("check ping error:{}", e.to_string());
                };
                ping_last_check_time = now_time;
            }

            //定时检查路由是否发生变动
            if self.config.check_is_mp(){
                if now_time - route_last_check_time >= 1000 {
                    if let Err(e) = self.check_route_for_platform().await{
                        error!("Check route change error:{}", e.to_string());
                    }

                    if let Err(e) = self.check_route_for_mgr().await{
                        error!("Check mgr route error:{}", e.to_string());
                    }
                    route_last_check_time = now_time;
                }
            }
            delay_for(Duration::from_millis(50)).await;
        }
    }

    /// 检查mgr主从变化, 这里只对mgr集群进行检测
    async fn check_route_for_mgr(&mut self) -> Result<()>{
        let mut plaform_node_info = self.platform_node_info.clone();
        for platform_node in &mut plaform_node_info{
            if platform_node.mgr{
                if let (Some(mut platform_pool), _) = self.get_platform_pool(&platform_node.platform).await{
                    let new_route_info = platform_pool.get_mgr_cluster_role_state(&platform_node.platform).await?;
                    if new_route_info.write.host != "".to_string(){
                        self.alter_platform_pool(&new_route_info, platform_node).await?;
                    }
                }
            }
        }
        self.platform_node_info = plaform_node_info;
        Ok(())
    }

    ///检查路由信息变动
    async fn check_route_for_platform(&mut self) -> Result<()> {
        let ha_ser_route = mysql_mp::get_platform_route(&self.config).await?;
        debug!("{:?}", &ha_ser_route);
        for route_info in ha_ser_route.value.route{
            let mut plaform_node_info = self.platform_node_info.clone();
            for platform_node in &mut plaform_node_info{
                if platform_node.platform == route_info.cluster_name{
                    self.alter_platform_pool(&route_info, platform_node).await?;
                }
            }
            self.platform_node_info = plaform_node_info;
        }
        Ok(())
    }

    async fn alter_platform_pool(&mut self, route_info: &RouteInfo, platform_node: &mut PlatforNodeInfo) -> Result<()>{
        if platform_node.check(&route_info){
            //发生变动， 开始修改连接池
            debug!("check after: {:?}", &platform_node);
            if let (Some(mut platform_pool), _) = self.get_platform_pool(&platform_node.platform).await{
                platform_pool.alter_pool(&platform_node).await?;
                platform_node.reset_alter();
            }
        }
        Ok(())
    }

    /// 对连接进行心跳检测、连接池维护
    async fn check_health_for_type(&mut self, check_type: HealthType) -> Result<()> {
        let platform_list = self.get_platform_list().await;
        for platform in platform_list{
            if let (Some(mut platform_pool), _) = self.get_platform_pool(&platform).await{
                platform_pool.check_health_for_type(&check_type).await?;
            }
        }
        Ok(())
    }

    async fn get_platform_list(&mut self) -> Vec<String> {
        let platform_pool_lock = self.platform_pool.lock().await;
        let mut platform_list = vec![];
        for platform in platform_pool_lock.keys(){
            platform_list.push(platform.clone());
        }
        return platform_list
    }

    /// 修改连接池最小/最大值
    pub async fn alter_pool_thread(&mut self, set_struct: &admin::SetStruct) -> Result<()> {
        if let Some(platform) = &set_struct.platform{
            if let (Some(mut platform_pool), _) = self.get_platform_pool(platform).await{
                if let Some(host_info) = &set_struct.host_info{
                    // 获取某一个节点连接池进行操作
                    platform_pool.alter_set_value(host_info, set_struct).await?;
                }else {
                    //对应platform下面所有节点进行操作
                    let node_list = platform_pool.get_node_list().await;
                    for host_info in node_list{
                        platform_pool.alter_set_value(&host_info, set_struct).await?;
                    }
                }
            }else {
                let err = "there is no connection pool for platfrom".to_string();
                return Err(Box::new(MyError(err.into())));
            }
        }else {
            let err = "platform is not empty".to_string();
            return Err(Box::new(MyError(err.into())));
        }
        Ok(())
    }


    /// 获取所有连接池状态信息
    pub async fn show_pool_state(&mut self, show_struct: &admin::ShowStruct) -> Result<admin::ShowState> {
        let mut show_state = vec![];
        if let Some(platform) = &show_struct.platform{
            let pool_state = self.get_state(&platform).await?;
            show_state.push(pool_state);
        }else {
            let platform_list = self.get_platform_list().await;
            for platform in platform_list{
                let pool_state = self.get_state(&platform).await?;
                show_state.push(pool_state);
            }
        }
        Ok(admin::ShowState{ platform_state: show_state })
    }

    /// 获取某一个platform下的连接池状态信息
    async fn get_state(&mut self, platform: &String) -> Result<admin::PoolState>{
        if let (Some(mut platform_pool), _) = self.get_platform_pool(platform).await{
            return Ok(platform_pool.get_pool_state(platform).await?)
        }
        return Err(Box::new(MyError(format!("there is no connection pool for platfrom: {}", platform).into())));
    }
}

/// 一个业务所有读写节点连接池
///
/// 所有节点连接池放于conn_pool这个hashmap中，以host_info作为key, 连接池作为value
///
/// write： 存放当前master节点host_info
///
/// read： 以vec存放当前slave和master节点host_info
///
/// is_alter： 记录当前主从关系版本号，如果发生改变则加一，获取到的线程会通过这个值的改变判断是否重新获取连接
#[derive(Clone, Debug)]
pub struct ConnectionsPoolPlatform{
    pub conn_pool: Arc<Mutex<HashMap<String, ConnectionsPool>>>,    //所有节点连接池, 以host_info作为key
    pub write: Arc<RwLock<Vec<String>>>,
    pub read: Arc<RwLock<Vec<String>>>,                             //存放可读节点信息，其中包括写入节点的信息，因为写几点也可读
    pub is_alter: Arc<AtomicUsize>,                                 //当前主从关系版本号， 加1表示发生变动，用于同步连接池状态，如果发生变动写操作需要判断使用的连接是否准确
    pub platform_config: Option<Platform>,
    pub questions:  Arc<AtomicUsize>,                               //记录当前业务集群所有请求次数
}

impl ConnectionsPoolPlatform{
    pub fn new(platform_config: &Platform) -> Result<ConnectionsPoolPlatform> {
        let mut conn_pool = HashMap::new();
        let mut my_config = Config::new(platform_config);
        debug!("node pool config: {:?}", &my_config);
        my_config.host_info = platform_config.get_write_host();
        let mut read_list: Vec<String> = vec![];

        //创建主库连接池，并放入conn_pool
        let write_config = my_config.clone();
        debug!("write node config: {:?}", &write_config);
        let write_pool = ConnectionsPool::new(&write_config)?;
        conn_pool.insert(my_config.host_info.clone(), write_pool);
        read_list.push(my_config.host_info.clone());
        let write = Arc::new(RwLock::new(vec![my_config.host_info.clone()]));

        //遍历slave节点并创建对应连接池，放入conn_pool
        if let Some(a) = &platform_config.read{
            for read_host in a{
                my_config.host_info = read_host.clone();
                debug!("read node config: {:?}", &my_config);
                read_list.push(read_host.clone());
                let read_pool = ConnectionsPool::new(&my_config)?;
                conn_pool.insert(my_config.host_info.clone(), read_pool);
            }
        }
        debug!("read node list:{:?}", &read_list);
        let read = Arc::new(RwLock::new(read_list));

        let is_alter = Arc::new(AtomicUsize::new(0));
        Ok(
            ConnectionsPoolPlatform{
                conn_pool: Arc::new(Mutex::new(conn_pool)),
                write,
                read,
                is_alter,
                platform_config: Some(platform_config.clone()),
                questions: Arc::new(AtomicUsize::new(0))
            }
        )
    }

    pub fn default() -> ConnectionsPoolPlatform{
        ConnectionsPoolPlatform{
            conn_pool: Arc::new(Mutex::new(HashMap::new())),
            write: Arc::new(RwLock::new(vec![])),
            read: Arc::new(RwLock::new(vec![])),
            is_alter: Arc::new(AtomicUsize::new(0)),
            platform_config: None,
            questions: Arc::new(AtomicUsize::new(0))
        }
    }

    /// 主从关系发生变化进行连接池对应关系变动
    async fn alter_pool(&mut self, platfor_node: &PlatforNodeInfo) -> Result<()> {
        if platfor_node.write_is_alter{
            //主从发生变化，新master必然会是已存在的slave， 所以直接设置master节点信息
            let mut write_host_lock = self.write.write().await;
            *write_host_lock = vec![platfor_node.write.clone()];
        }

        //读节点变动
        if platfor_node.read_is_alter{
            //let mut new_read_list = vec![];
            let new_read_list = platfor_node.read.clone();

            //new_read_list.push(platfor_node.write.clone());
            //首先进行新增判断
            let mut read_host_lock = self.read.write().await;
            'aa: for host_info in &new_read_list{
                for info in &*read_host_lock{
                    if info == host_info{
                        continue 'aa;
                    }
                }
                //不存在当前读取列表中，增加连接池
                //self.new_pool(&host_info).await?;
                if let Some(conf) = &self.platform_config{
                    let mut my_config = Config::new(&conf);
                    my_config.host_info = host_info.clone();
                    let new_pool = ConnectionsPool::new(&my_config)?;
                    let mut pool_lock = self.conn_pool.lock().await;
                    pool_lock.insert(host_info.clone(), new_pool);
                    read_host_lock.push(host_info.clone());
                }else {
                    let err = String::from("new connection pool, but no configuration information found");
                    info!("{}",err);
                    info!("{}", String::from("failed......"));
                }
            }

            //反向判断现有节点是否存在新路由关系中，如果不存在则删除
            'a: for info in &*read_host_lock{
                for host_info in &new_read_list{
                    if info == host_info{
                        continue 'a;
                    }
                }
                //不能存在则删除
                let mut pool_lock = self.conn_pool.lock().await;
                match pool_lock.remove(info){
                    Some(mut conn_pool) => {
                        conn_pool.close_pool().await;
                    }
                    None => {}
                }
                //self.drop_pool(&info).await?;
            }
            *read_host_lock = new_read_list;
        }

        Ok(())
    }

    /// 对mgr集群获取节点状态信息
    ///
    /// 使用的连接会一直缓存，在连接获取时使用的最小连接算法，可能会随机到任意一个节点
    ///
    /// 可能会在每个节点缓存一条连接， 这样可以保证在连接使用完的最坏情况下依然能检查状态
    async fn get_mgr_cluster_role_state(&mut self, platform: &String) -> Result<RouteInfo>{
        let cache_key = "mgrclusterrolestate_bbb".to_string();
        let (mut conn_info, mut conn_pool) = self.get_pool(&SqlStatement::Query, &cache_key, &None, platform, false).await?;
        if &conn_info.cached == &"".to_string(){
            conn_info.cached = cache_key;
        }
        let sql = String::from("select member_host,member_port,member_state,member_role from performance_schema.replication_group_members;");
        let result = conn_info.execute_command(&sql).await?;
        conn_pool.return_pool(conn_info, platform).await?;
        // self.return_pool(conn_info, 0, platform).await?;
        Ok(RouteInfo::new_mgr_route(&result, platform))
    }

    /// 修改ops计数状态
    pub async fn save_com_state(&mut self, host_info: &String, sql_type: &SqlStatement) -> Result<()> {
        debug!("save operation count:{:?} for {:?}", sql_type, host_info);
        self.questions.fetch_add(1, Ordering::SeqCst);
        if let Some(mut node_pool) = self.get_node_pool(host_info).await{
            //info!("save operation count:{:?} for {:?}", sql_type, host_info);
            node_pool.save_ops_info(sql_type).await;
        }
        Ok(())
    }

    /// 通过sql类型判断从总连接池中获取对应读/写连接
    pub async fn get_pool(&mut self, sql_type: &SqlStatement, key: &String,
                          select_comment: &Option<String>,
                          platform: &String, is_sublist: bool) -> Result<(MysqlConnectionInfo, ConnectionsPool)> {
        return match sql_type {
            SqlStatement::AlterTable |
            SqlStatement::Create |
            SqlStatement::Update |
            SqlStatement::Insert |
            SqlStatement::Drop |
            SqlStatement::Delete |
            SqlStatement::StartTransaction => {
                Ok(self.get_write_conn(key, platform, is_sublist).await?)
            }
            SqlStatement::Prepare => {
                // prepare类型仅当以及prepare之后执行操作，所以这里通过get_read_conn获取
                // 因为get_read_conn会首先从所有节点连接池中获取cached连接
                Ok(self.get_read_conn(key, platform, is_sublist).await?)
            }
            _ => {
                if let Some(comment) = select_comment {
                    if comment.to_lowercase() == String::from("force_master") {
                        Ok(self.get_write_conn(key, platform, is_sublist).await?)
                    } else {
                        Ok(self.get_read_conn(key, platform, is_sublist).await?)
                    }
                } else {
                    //获取读连接
                    Ok(self.get_read_conn(key, platform, is_sublist).await?)
                }
            }
        }
//        let error = "no available connection".to_string();
//        return Err(Box::new(MyError(error.into())));
    }

    // /// 归还连接到对应节点的连接池中
    // pub async fn return_pool(&mut self, mut mysql_conn: MysqlConnectionInfo, seq: u8, platform: &String) -> Result<()>{
    //     mysql_conn.check_rollback(seq).await?;
    //
    //     //mysql_conn.reset_cached().await?;
    //     mysql_conn.reset_conn_default()?;
    //
    //     let mut conn_pool_lock = self.conn_pool.lock().await;
    //     let host_info = mysql_conn.host_info.clone();
    //     match conn_pool_lock.remove(&host_info){
    //         Some(mut conn_pool) => {
    //             conn_pool.return_pool(mysql_conn, platform).await?;
    //             conn_pool_lock.insert(host_info, conn_pool);
    //         }
    //         None => {}
    //     }
    //     return Ok(());
    // }

    /// 获取缓存的连接
    async fn get_cached_conn(&mut self, key: &String) -> Result<(Option<MysqlConnectionInfo>, Option<ConnectionsPool>)> {
        let mut conn_pool_lock = self.conn_pool.lock().await;
        let read_list_lock = self.read.read().await;
        for read_host_info in &*read_list_lock{
            match conn_pool_lock.remove(read_host_info){
                Some(mut conn_pool) => {
                    conn_pool_lock.insert(read_host_info.clone(), conn_pool.clone());
                    if let Some(v) = conn_pool.check_cache(key).await?{
                        return Ok((Some(v), Some(conn_pool)))
                    }else {
                    }
                }
                None => {}
            }
        }
        return Ok((None, None))
    }

    /// 获取主节点连接
    async fn get_write_conn(&mut self, key: &String, platform: &String, is_sublist: bool) -> Result<(MysqlConnectionInfo, ConnectionsPool)>{
        let mut conn_pool_lock = self.conn_pool.lock().await;
        let write_host_lock = self.write.read().await;
        for write_host_info in &*write_host_lock{
            match conn_pool_lock.remove(write_host_info){
                Some(mut conn_pool) => {
                    let conn_info_result = conn_pool.get_pool(key, platform, is_sublist).await;
                    conn_pool_lock.insert(write_host_info.clone(), conn_pool.clone());
                    match conn_info_result{
                        Ok(conn_info) => {
                            return Ok((conn_info, conn_pool));
                        }
                        Err(e) => {
                            return Err(Box::new(MyError(e.to_string().into())));
                        }
                    }

                }
                None => {}
            }
        }
        let error = "no available connection".to_string();
        error!("get write connection error: {}", error);
        return Err(Box::new(MyError(error.into())));
    }

    /// 通过最少连接数获取连接
    async fn get_read_conn(&mut self, key: &String, platform: &String, is_sublist: bool) -> Result<(MysqlConnectionInfo, ConnectionsPool)>{
        if let (Some(conn), Some(conn_pool)) = self.get_cached_conn(key).await?{
            return Ok((conn, conn_pool));
        }

        let mut conn_pool_lock = self.conn_pool.lock().await;
        let read_list_lock = self.read.read().await;

        let mut active_count = 0 as usize;
        let mut start = false;
        //存储最小连接的连接池key值，最终从这个连接池中获取连接
        let mut tmp_key = None;
        for read_host_info in &*read_list_lock{
            match conn_pool_lock.get(read_host_info){
                Some(v) => {
                    let tmp_count = v.active_count.load(Ordering::SeqCst);

                    // 判断当前连接池大小, 如果小于最小连接池大小，则有可能是节点异常，排除该节点
                    let tmp_queued_count = v.queued_count.load(Ordering::Relaxed);
                    let tmp_cached_count = v.cached_count.load(Ordering::Relaxed);
                    let tmp_min = v.min_thread_count.load(Ordering::Relaxed);
                    if tmp_queued_count + tmp_count + tmp_cached_count < tmp_min {
                        continue;
                    }

                    //开始
                    if !start{
                        active_count = tmp_count;
                        start = true;
                        tmp_key = Some(read_host_info.clone());
                    }

                    //获取连接池的活跃连接，对比上一个的活跃数，active_count始终存储最小的活跃连接值
                    if tmp_count < active_count {
                        active_count = tmp_count;
                        tmp_key = Some(read_host_info.clone())
                    }
//                    conn_pool_lock.insert(read_host_info.clone(), v);
                },
                None => {}
            }
        }
        match tmp_key{
            Some(v) => {
                match conn_pool_lock.remove(&v){
                    Some(mut conn_pool) => {
                        let conn_info_result = conn_pool.get_pool(key, platform, is_sublist).await;
                        conn_pool_lock.insert(v.clone(), conn_pool.clone());
                        return match conn_info_result {
                            Ok(conn_info) => {
                                Ok((conn_info, conn_pool))
                            }
                            Err(e) => {
                                Err(Box::new(MyError(e.to_string().into())))
                            }
                        }
                    }
                    None => {}
                }
            }
            None => {}
        }
        let error = "no available connection".to_string();
        error!("get read connection error: {}", error);
        return Err(Box::new(MyError(error.into())));
    }

    /// 检查当前连接和当前执行的语句是否匹配
    ///
    /// 用于已经获取到连接的时候，因为可能刚才执行的select， select是可以路由到任何节点的
    ///
    /// 如果现在是需要执行update则需要获取写节点的连接
    ///
    /// 返回true表示可以执行该sql语句，如果为false则需要重新获取连接
    pub async fn conn_type_check(&mut self, host_info: &String, sql_type: &SqlStatement, select_comment: &Option<String>) -> Result<bool> {
        let read_list_lock = self.read.read().await;
        let write_list_lock = self.write.read().await;
        //判断是否为写节点的连接，如果为写节点的连接可以执行任何操作
        for write_host_info in &*write_list_lock{
            if write_host_info == host_info{
                return Ok(true)
            }
        }

        //上面已经判断过是否为写节点连接， 到这里表示肯定为读节点连接，
        //判断sql类型的同时还需要判断该连接节点是否存在读列表中
        //如果不存在则表示该连接节点宕机则需要重新获取
        for read_host_info in &*read_list_lock{
            if read_host_info == host_info{
                return match sql_type {
                    SqlStatement::AlterTable |
                    SqlStatement::StartTransaction |
                    SqlStatement::Delete |
                    SqlStatement::Drop |
                    SqlStatement::Insert |
                    SqlStatement::Update |
                    SqlStatement::Create => {
                        Ok(false)
                    }
                    _ => {
                        if let Some(v) = select_comment {
                            if v.to_lowercase() == String::from("force_master") {
                                return Ok(false);
                            }
                        }
                        Ok(true)
                    }
                }
            }
        }
        return Ok(false)
    }

    /// clone某个节点的连接池
    async fn get_node_pool(&mut self, host_info: &String) -> Option<ConnectionsPool>{
        let pool_lock = self.conn_pool.lock().await;
        if let Some(node_pool) = pool_lock.get(host_info){
            return Some(node_pool.clone())
        }
        return None
    }

    /// 对连接检查心跳、连接池维护
    async fn check_health_for_type(&mut self, check_type: &HealthType) -> Result<()> {
        let node_list = self.get_node_list().await;
        for host_info in node_list{
            if let Some(mut node_pool) = self.get_node_pool(&host_info).await{
                match check_type{
                    HealthType::Ping => node_pool.check_ping().await?,
                    HealthType::Maintain => node_pool.maintain_pool().await?,
                }
            }
        }
        Ok(())
    }

    async fn get_node_list(&mut self) -> Vec<String> {
        let pool_lock = self.conn_pool.lock().await;
        let mut node_list = vec![];
        for host_info in pool_lock.keys(){
            node_list.push(host_info.clone());
        }
        return node_list
    }

    /// 通过set命令修改连接池部分信息
    async fn alter_set_value(&mut self, host_info: &String, set_struct: &admin::SetStruct) -> Result<()>{
        if let Some(mut conn_pool) = self.get_node_pool(host_info).await{
            conn_pool.alter_thread(&set_struct.set_variables).await?;
        }else {
            let err = format!("there is no connection pool for {}", host_info);
            return Err(Box::new(MyError(err.into())));
        }
        Ok(())
    }

    /// 获取连接池状态
    async fn get_pool_state(&mut self, platform: &String) -> Result<admin::PoolState> {
        let mut pool_state = admin::PoolState{
            platform: platform.clone(),
            write_host: "".to_string(),
            read_host: vec![],
            questions: self.questions.load(Ordering::Relaxed),
            host_state: vec![]
        };
        self.get_node_role(&mut pool_state).await;

        let node_list = self.get_node_list().await;
        for host_info in node_list{
            if let Some(mut node_pool) = self.get_node_pool(&host_info).await{
                let one_pool_state = node_pool.get_pool_state(&host_info).await;
                pool_state.host_state.push(one_pool_state)
            }
        }
        return Ok(pool_state)
    }

    /// 获取当前业务后端读写节点关系
    ///
    /// 这里为了管理端的准确性从连接池获取
    async fn get_node_role(&mut self, pool_state: &mut admin::PoolState) {
        let write_lock = self.write.read().await;
        let read_lock = self.read.read().await;
        for write_host in &*write_lock{
            pool_state.write_host = write_host.clone();
        }
        for read_host in &*read_lock{
            pool_state.read_host.push(read_host.clone());
        }
    }

}

/// 连接池管理
///
/// conn_queue存放所有未缓存的空闲连接
///
/// cached_queue存放被连接缓存的mysql连接， 该缓存是由于事务未完成，所以放到此处不让别的连接使用，
/// 如果事务完成会放回conn_queue队列中。
///
/// queued_count 加 cached_count 加 active_count 是当前连接池中所有的连接数
/// 
/// fush: 是否开启单个platform熔断功能， 即单个platform使用的连接超过总连接池80%则不再分配连接，
/// 根据platform_conn_count记录的分配数据进行判断
#[derive(Debug, Clone)]
pub struct ConnectionsPool {
    conn_queue: Arc<Mutex<ConnectionInfo>>,                 //所有连接队列
    cached_queue: Arc<Mutex<HashMap<String, MysqlConnectionInfo>>>,       //已缓存的连接
    cached_count: Arc<AtomicUsize>,                              //当前缓存的连接数
    queued_count: Arc<AtomicUsize>,                              //当前连接池总连接数
    active_count: Arc<AtomicUsize>,                              //当前活跃连接数
    com_select:  Arc<AtomicUsize>,
    com_update:  Arc<AtomicUsize>,
    com_delete:  Arc<AtomicUsize>,
    com_insert:  Arc<AtomicUsize>,
    min_thread_count: Arc<AtomicUsize>,                          //代表连接池最小线程
    max_thread_count: Arc<AtomicUsize>,                          //最大线连接数
    panic_count: Arc<AtomicUsize>,                               //记录发生panic连接的数量
    node_role: Arc<AtomicBool>,                                  //主从角色判断，true为主，flase为从
    node_state: Arc<AtomicBool>,                                 //节点状态，如果为flase表示宕机
    host_info: Arc<Mutex<String>>,                                //记录节点信息
    auth: Arc<AtomicBool>,                                       //是否打开sql审计功能
    fuse: Arc<AtomicBool>,                                       //是否开启熔断自我保护功能, 单个子platform连接占用率超过80%就不会再分配连接
    platform_conn_count: Arc<Mutex<HashMap<String, usize>>>,     //记录所有子platform连接分配数
}

impl ConnectionsPool{
    pub fn new(conf: &Config) -> Result<ConnectionsPool>{
        debug!("create connection pool for {}", &conf.host_info);
        let queue = ConnectionInfo::new(conf)?;
        let queued_count = Arc::new(AtomicUsize::new(queue.pool.len()));
        let conn_queue = Arc::new(Mutex::new(queue));
        Ok(ConnectionsPool{
            conn_queue,
            cached_queue: Arc::new(Mutex::new(HashMap::new())),
            cached_count: Arc::new(AtomicUsize::new(0)),
            queued_count,
            active_count: Arc::new(AtomicUsize::new(0)),
            com_select: Arc::new(AtomicUsize::new(0)),
            com_update: Arc::new(AtomicUsize::new(0)),
            com_delete: Arc::new(AtomicUsize::new(0)),
            com_insert: Arc::new(AtomicUsize::new(0)),
            min_thread_count: Arc::new(AtomicUsize::new(conf.min)),
            max_thread_count: Arc::new(AtomicUsize::new(conf.max)),
            panic_count: Arc::new(AtomicUsize::new(0)),
            node_role: Arc::new(AtomicBool::new(true)),
            node_state: Arc::new(AtomicBool::new(true)),
            host_info: Arc::new(Mutex::new(conf.host_info.clone())),
            auth: Arc::new(AtomicBool::new(false)),
            fuse: Arc::new(AtomicBool::new(false)),
            platform_conn_count: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn save_ops_info(&mut self, sql_type: &SqlStatement) {
        match sql_type{
            SqlStatement::Update => {self.com_update.fetch_add(1, Ordering::SeqCst);},
            SqlStatement::Insert => {self.com_insert.fetch_add(1, Ordering::SeqCst);},
            SqlStatement::Delete => {self.com_delete.fetch_add(1, Ordering::SeqCst);},
            SqlStatement::Query => {self.com_select.fetch_add(1, Ordering::SeqCst);},
            _ =>{}
        }
    }

    async fn alter_thread(&mut self, set_type: &admin::SetVariables) -> Result<()>{
        match set_type{
            admin::SetVariables::MinThread(value) => {
                self.min_thread_count.store(value.clone(), Ordering::Relaxed);
            }
            admin::SetVariables::MaxThread(value) => {
                self.max_thread_count.store(value.clone(), Ordering::Relaxed);
            }
            admin::SetVariables::Auth(value) => {
                if value == &0{
                    self.auth.store(false, Ordering::Relaxed);
                }else {
                    self.auth.store(true, Ordering::Relaxed);
                }
            }
            admin::SetVariables::Fuse(value) => {
                if value == &0{
                    self.fuse.store(false, Ordering::Relaxed);
                }else {
                    self.fuse.store(true, Ordering::Relaxed);
                }
            }
            _ => {
                let err = "only support set min_thread/max_thread/auth".to_string();
                return Err(Box::new(MyError(err.into())));
            }
        }
        Ok(())
    }

    /// 检查是否开启熔断机制，如果开启则判断该paltform请求的连接数是否已达到80%, 超过限制则返回true
    ///
    /// 没有开启、或者开启没有达到阈值则增加记录
    async fn check_fuse(&mut self, platform: &String, is_sublist: bool) -> Result<bool>{
        debug!("check_fuse: platform {}, is_sublist: {}, is fuse:{}", platform, is_sublist, &self.fuse.load(Ordering::Relaxed));
        if !is_sublist {
            return Ok(false)
        }
        return if !self.fuse.load(Ordering::Relaxed) {
            self.alter_platform_conn_count(platform).await?;
            Ok(false)
        } else {
            let mut platform_conn_count_lock = self.platform_conn_count.lock().await;
            match platform_conn_count_lock.remove(platform) {
                Some(v) => {
                    debug!("check fuse status: platform {} count {}", platform, v);
                    if (v as f64 / self.max_thread_count.load(Ordering::Relaxed) as f64) * 100 as f64 > 80 as f64 {
                        platform_conn_count_lock.insert(platform.clone(), v);
                        Ok(true)
                    } else {
                        platform_conn_count_lock.insert(platform.clone(), v + 1);
                        Ok(false)
                    }
                },
                None => {
                    platform_conn_count_lock.insert(platform.clone(), 1);
                    Ok(false)
                }
            }
        }
    }

    async fn alter_platform_conn_count(&mut self, platform: &String) -> Result<()>{
        debug!("add fuse status: platform {}", platform);
        let mut platform_conn_count_lock = self.platform_conn_count.lock().await;
        match platform_conn_count_lock.remove(platform) {
            Some(v) => {
                platform_conn_count_lock.insert(platform.clone(), v + 1);
                Ok(())
            },
            None => {
                platform_conn_count_lock.insert(platform.clone(), 1);
                Ok(())
            }
        }
    }

    /// 归还连接时对记录减法
    async fn return_platform_conn_count(&mut self, platform: &String) -> Result<()>{
        let mut platform_conn_count_lock = self.platform_conn_count.lock().await;
        match platform_conn_count_lock.remove(platform) {
            Some(v) => {
                if v > 0{
                    platform_conn_count_lock.insert(platform.clone(), v - 1);
                }
                Ok(())
            },
            None => {
                Ok(())
            }
        }
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
    pub async fn get_pool(&mut self, key: &String, platform: &String, is_sublist: bool) -> Result<MysqlConnectionInfo> {
        let std_duration = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
        let start_time = Duration::from(std_duration);
        let wait_time = Duration::from_millis(1000);    //等待时间1s
        match self.check_cache(key).await?{
            Some(conn) => {
                return Ok(conn);
            }
            None => {
                //检查熔断机制
                if self.check_fuse(platform, is_sublist).await?{
                    error!("{} connection access has reached the upper limit set by the system", platform);
                    return Err(Box::new(MyError(format!("{} connection access has reached the upper limit set by the system", platform).into())));
                }
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
                                self.return_platform_conn_count(platform).await?;
                                return Err(Box::new(MyError(String::from("get connection timeout").into())));
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
    pub async fn return_pool(&mut self, mut conn: MysqlConnectionInfo, platform: &String) -> Result<()> {
        if conn.cached == String::from(""){
            self.return_platform_conn_count(platform).await?;
            //let &(ref pool, ref condvar) = &*self.conn_queue;
            let mut pool = self.conn_queue.lock().await;
            match conn.check_health().await{
                Ok(b) =>{
                    if b{
                        pool.pool.push_front(conn);
                        //pool.pool.push_back(conn);
                        self.queued_count.fetch_add(1, Ordering::SeqCst);
                        self.active_count.fetch_sub(1, Ordering::SeqCst);
                    }else {
                        self.active_count.fetch_sub(1, Ordering::SeqCst);
                        error!("return connection, but this connection {}",String::from("check ping failed"));
                    }
                }
                Err(e) => {
                    self.active_count.fetch_sub(1, Ordering::SeqCst);
                    error!("{}",format!("return connection, but this connection : {:?}", e.to_string()));
                }
            }
        }else {
            let mut cache_pool = self.cached_queue.lock().await;
            cache_pool.insert(conn.cached.clone(), conn);
            self.cached_count.fetch_add(1, Ordering::SeqCst);
            self.active_count.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(())
    }

    /// 减少连接池活跃连接计数，在client处理连接检查异常时对计数进行减少
    /// 异常连接不需要归还，所以只做计数操作
    pub async fn sub_active_count(&mut self) {
        self.active_count.fetch_sub(1, Ordering::SeqCst);
    }

    async fn get_pool_state(&mut self, host_info: &String) -> admin::HostPoolState{
        let platform_conn_count_lock = self.platform_conn_count.lock().await;
        admin::HostPoolState{
            host_info: host_info.clone(),
            com_select: self.com_select.load(Ordering::Relaxed),
            com_update: self.com_update.load(Ordering::Relaxed),
            com_delete: self.com_delete.load(Ordering::Relaxed),
            com_insert: self.com_insert.load(Ordering::Relaxed),
            min_thread: self.min_thread_count.load(Ordering::Relaxed),
            max_thread: self.max_thread_count.load(Ordering::Relaxed),
            thread_count: self.queued_count.load(Ordering::Relaxed),
            cached_count: self.cached_count.load(Ordering::Relaxed),
            active_thread: self.active_count.load(Ordering::Relaxed),
            auth: self.auth.load(Ordering::Relaxed),
            fuse: self.fuse.load(Ordering::Relaxed),
            platform_conn_count: platform_conn_count_lock.clone()
        }
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

    /// 关闭连接池
    async fn close_pool(&mut self) {
        let mut pool_lock = self.conn_queue.lock().await;
        if let Some(mut conn) = pool_lock.pool.pop_front() {
            conn.close();
        }
    }

    /// 维护线程池中线程的数量， 如果低于最小值补齐
    ///
    /// 如果大于最小值，则进行空闲时间检测，空闲时间超过600s的将断开
    ///
    /// 不会操作cached队列
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
                        self.active_count.fetch_sub(1, Ordering::SeqCst);
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

    /// 对连接池中的连接进行心跳检查
    pub async fn check_ping(&mut self) -> Result<()> {
        self.check_cached_ping().await?;

        if self.queued_count.load(Ordering::Relaxed) > 0 {
            let mut pool = self.conn_queue.lock().await;
            for _ in 0..pool.pool.len() {
                if let Some(mut conn) = pool.pool.pop_front(){
                    self.queued_count.fetch_sub(1, Ordering::SeqCst);
                    self.active_count.fetch_add(1, Ordering::SeqCst);
                    match conn.check_health().await{
                        Ok(b) =>{
                            if b{
                                pool.pool.push_back(conn);
                                self.queued_count.fetch_add(1, Ordering::SeqCst);
                                self.active_count.fetch_sub(1, Ordering::SeqCst);
                            }else {
                                self.active_count.fetch_sub(1, Ordering::SeqCst);
                                error!("{}",String::from("check ping failed"));
                            }
                        }
                        Err(e) => {
                            self.active_count.fetch_sub(1, Ordering::SeqCst);
                            error!("{}",format!("check ping error: {:?}", e.to_string()));
                        }
                    }
                }
            }
            drop(pool);
        }
        Ok(())
    }

    /// 对cached列表进行心跳检测
    pub async fn check_cached_ping(&mut self) -> Result<()> {
        if self.cached_count.load(Ordering::Relaxed) > 0 {
            let cache_key = self.get_cached_key().await;
            let mut cache_pool = self.cached_queue.lock().await;
            for key in cache_key{
                match cache_pool.remove(&key){
                    Some(mut conn) => {
                        self.cached_count.fetch_sub(1, Ordering::SeqCst);
                        self.active_count.fetch_add(1, Ordering::SeqCst);
                        match conn.check_health().await{
                            Ok(b) =>{
                                if b{
                                    cache_pool.insert(key.clone(), conn);
                                    self.cached_count.fetch_add(1, Ordering::SeqCst);
                                    self.active_count.fetch_sub(1, Ordering::SeqCst);
                                }else {
                                    self.active_count.fetch_sub(1, Ordering::SeqCst);
                                    error!("{}",String::from("check ping failed"));
                                }
                            }
                            Err(e) => {
                                self.active_count.fetch_sub(1, Ordering::SeqCst);
                                error!("{}",format!("check ping error: {:?}", e.to_string()));
                            }
                        }
                    },
                    None => {}
                }
            }
            drop(cache_pool);
        }
        Ok(())
    }

    async fn get_cached_key(&mut self) -> Vec<String> {
        let cache_pool = self.cached_queue.lock().await;
        let cached_keys = cache_pool.keys();
        let mut tmp = vec![];
        for key in cached_keys{
            tmp.push(key.clone());
        }
        tmp
    }

    pub async fn auth_save(&self, sql: &String, host: &String) {
        if self.auth.load(Ordering::Relaxed){
            info!("{:?} -> {:?}",host, sql);
        }
    }

}

#[derive(Debug)]
pub struct MysqlConnectionInfo {
    pub conn: TcpStream,
    pub cached: String,             //记录使用该连接的线程hash，用于prepare和需要固定连接的操作
    pub last_time: usize,          //记录该mysql连接最后执行命令的时间，用于计算空闲时间，如果没有设置缓存标签在达到200ms空闲时将放回连接池
    pub is_transaction: bool,        //记录是否还有事务或者锁存在
    pub is_write: bool,            //是否为写入连接
    pub host_info: String
}
impl MysqlConnectionInfo{
    pub fn new(conn: MysqlConnection, config: &Config) -> MysqlConnectionInfo{
        MysqlConnectionInfo{
            conn: conn.conn,
            cached: "".to_string(),
            last_time: 0,
            is_transaction: false,
            is_write: false,
            host_info: config.host_info.clone()
        }
    }

    pub fn try_clone(&self) -> Result<MysqlConnectionInfo>{
        Ok(MysqlConnectionInfo{
            conn: self.conn.try_clone()?,
            cached: self.cached.clone(),
            last_time: self.last_time.clone(),
            is_transaction: self.is_transaction.clone(),
            is_write: self.is_write.clone(),
            host_info: self.host_info.clone()
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

//    /// 检查事务超时，如果超过600s则返回true
//    pub fn check_transaction_timeout(&mut self) -> bool {
//        if self.is_transaction{
//            return self.check_sleep();
//        }
//        return false;
//    }

    /// 检查空闲时间，超过100s返回true
    pub fn check_sleep(&mut self) -> bool {
        let dt = Local::now();
        let now_time = dt.timestamp_millis() as usize;
        if now_time - self.last_time > 100000 {
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

    pub async fn send_packet_only(&mut self, packet: &Vec<u8>) -> Result<()> {
        self.conn.write_all(packet)?;
        Ok(())
    }

    pub async fn response_for_larger_packet(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let (buf, header) = self.get_packet_from_stream().await?;
        Ok((buf, header))
    }

    /// send packet and return response packet
    pub async fn send_packet(&mut self, packet: &Vec<u8>) -> Result<(Vec<u8>, PacketHeader)> {
        debug!("{}",crate::info_now_time(String::from("write all to mysql conn")));
        self.conn.write_all(packet)?;
        self.set_last_time();
        debug!("{}",crate::info_now_time(String::from("get mysql response")));
        let (buf, header) = self.get_packet_from_stream().await?;
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
                    let new_header = PacketHeader::new(&header_buf)?;
                    header.reset(&new_header);
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
    pub async fn get_packet_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let (mut buf,header) = self.get_from_stream().await?;
        while header.payload == 0xffffff{
            debug(header.payload);
            let (buf_tmp,_) = self.get_from_stream().await?;
            buf.extend(buf_tmp);
        }
        Ok((buf, header))
    }

    /// read on packet from socket
    async fn get_from_stream(&mut self) -> Result<(Vec<u8>, PacketHeader)>{
        let mut header_buf = vec![0 as u8; 4];
        let mut header: PacketHeader = PacketHeader { payload: 0, seq_id: 0 };
        loop {
            match self.conn.read_exact(&mut header_buf){
                Ok(_) => {
                    let new_header = PacketHeader::new(&header_buf)?;
                    header.reset(&new_header);
                    if header.payload > 0 {
                        break;
                    }
                }
                Err(e) => {
                    //debug(e);
                    let str_tmp = e.to_string();
                    error!("{}", &str_tmp);
                    if str_tmp.contains("Resource temporarily unavailable") {
                        continue;
                    }
                    return Err(Box::new(MyError(e.to_string().into())));
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

    pub fn rollback_no_commit(&mut self, seq: u8) -> Result<()>{
        let sql = String::from("rollback;");
        let packet_full = self.set_default_packet(&sql, seq);
        let (a, _b) = self.__send_packet(&packet_full)?;
        self.check_packet_is(&a)?;
        Ok(())
    }

    pub fn set_default_autocommit(&mut self, autocommit: u8) -> Result<()> {
        let sql = format!("set autocommit={};", autocommit);
        let packet_full = self.set_default_packet(&sql, 0);
        let (a, _b) = self.__send_packet(&packet_full)?;
        self.check_packet_is(&a)?;
        Ok(())
    }

    pub fn set_default_db(&mut self, db: String) -> Result<()> {
        let sql = format!("use {};", db);
        let packet_full = self.set_default_packet(&sql, 0);
        let (a, _b) = self.__send_packet(&packet_full)?;
        self.check_packet_is(&a)?;
        Ok(())
    }

    fn set_autocommit(&mut self) -> Result<()> {
        let sql = format!("set autocommit=1;");
        let packet_full = self.set_default_packet(&sql, 0);
        let (a, _b) = self.__send_packet(&packet_full)?;
        self.check_packet_is(&a)?;
        Ok(())
    }

    fn set_default_packet(&mut self, sql: &String, seq: u8) -> Vec<u8> {
        let mut packet = vec![];
        packet.push(3 as u8);
        packet.extend(sql.as_bytes());
        let mut packet_full = vec![];
        packet_full.extend(pack_header(&packet, seq));
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
    pub async fn check_health(&mut self) -> Result<bool> {
        let mut packet: Vec<u8> = vec![];
        packet.extend(readvalue::write_u24(1));
        packet.push(0);
        packet.push(0x0e);
        if let Err(e) = self.conn.write(&packet){
            error!("check ping write error: {}",e.to_string());
            return Ok(false);
        };
        match self.get_packet_from_stream().await{
            Ok((buf, _header)) => {
                if let Err(e) = self.check_packet_is(&buf){
                    error!("check ping response packet error: {}",e.to_string());
                    self.close();
                    return Ok(false);
                }
            }
            Err(e) => {
                error!("check health error: {}",e.to_string());
                return Ok(false);
            }
        }
        return Ok(true);

    }

    /// 初始化连接为默认状态
    ///
    /// 用于缓存线程归还到线程池时使用
    pub fn reset_conn_default(&mut self) -> Result<()>{
        self.set_autocommit()?;
        let sql = String::from("use information_schema");
        let packet_full = self.set_default_packet(&sql, 0);
        let (a, _b) = self.__send_packet(&packet_full)?;
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

    /// 执行sql语句并返回结果
    pub async fn execute_command(&mut self, sql: &String) -> Result<Vec<HashMap<String, String>>>{
        let packet = self.set_default_packet(&sql, 0);
        debug!("{}", sql);
        return Ok(self.unpack_text_packet(&packet).await?);
    }

    /// 获取返回结果
    async fn unpack_text_packet(&mut self, packet: &Vec<u8>) -> Result<Vec<HashMap<String, String>>> {
        let (packet, _) = self.__send_packet(&packet)?;
        self.check_packet_is(&packet)?;
        let mut values_info = vec![];   //数据值
        let mut column_info = vec![];   //每个column的信息

        let column_count = packet[0];
        for _ in 0..column_count {
            let (buf, _) = self.get_packet_from_stream().await?;
            let column = MetaColumn::new(&buf);
            column_info.push(column);
        }
        //开始获取返回数据
        let mut eof_num = 0;
        'b: loop {
            if eof_num > 1{
                break 'b;
            }
            let (buf, header) = self.get_packet_from_stream().await?;
            //println!("{},{}",buf[0], header.payload);
            if buf[0] == 0x00{
                if header.payload < 9{
                    eof_num += 1;
                    continue 'b;
                }
            }else if buf[0] == 0xfe {
                eof_num += 1;
                continue 'b;
            }else if buf[0] == 0xff {
                break;
            }
            let values = self.unpack_text_value(&buf, &column_info);
            values_info.push(values);
        }
        return Ok(values_info);
    }

    /// 解析行数据
    fn unpack_text_value(&self, buf: &Vec<u8>,column_info: &Vec<MetaColumn>) -> HashMap<String,String> {
        //解析每行数据
        let mut values_info = HashMap::new();
        let mut offset = 0;
        for cl in column_info.iter(){
            let value: String;
            let cl_name = cl.name.clone();
            let mut var_len = buf[offset] as usize;
            offset += 1;
            if var_len == 0xfc {
                var_len = readvalue::read_u16(&buf[offset..offset + 2]) as usize;
                offset += 2;
                value = readvalue::read_string_value(&buf[offset..offset + var_len]);
                offset += var_len;
            }
            else if var_len == 0xfd {
                var_len = readvalue::read_u24(&buf[offset..offset + 3]) as usize;
                offset += 3;
                value = readvalue::read_string_value(&buf[offset..offset + var_len]);
                offset += var_len;
            }
            else if var_len == 0xfe {
                var_len = readvalue::read_u64(&buf[offset..offset + 8]) as usize;
                offset += 8;
                value = readvalue::read_string_value(&buf[offset..offset + var_len]);
                offset += var_len;
            }
            else if var_len == 0xfb {
                value = String::from("");
            }
            else {
                value = readvalue::read_string_value(&buf[offset..offset + var_len]);
                offset += var_len;
            }


            values_info.insert(cl_name,value);
        }

        return values_info;
    }


    pub async fn check_rollback(&mut self, seq: u8) -> Result<()> {
        // 如果有事务存在则回滚， 回滚失败会结束该连接
        if self.is_transaction{
            self.is_transaction = false;
            if let Err(e) = self.rollback_no_commit(seq){
                info!("{:?}", e.to_string());
                self.close();
                return Ok(())
            }
        }
        return Ok(())
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
                let mut conn_info = MysqlConnectionInfo::new(conn, &self.config);
                conn_info.set_autocommit()?;
                self.pool.push_back(conn_info);
                Ok(())
            }
            Err(err) => Err(err)
        }
    }
}





