mod dbengine;
mod server;
mod mysql;
mod readvalue;
use structopt::StructOpt;
use std::fmt;
use chrono::Local;
use serde_derive::{Deserialize};
use std::fs::File;
use std::io::prelude::*;
use crate::server::mysql_mp::RouteInfo;
use crate::dbengine::client::ClassUseTimeAll;

/// 存储客户端使用user配置
///
/// 对应配置文件[[user]]模块
#[derive(Debug, Deserialize, Clone)]
pub struct UserConfig{
    pub user: String,
    pub password: String,
    pub platform: String
}

/// 总配置信息
///
#[derive(Debug, Deserialize, Clone)]
pub struct MyConfig {
    pub user: String,                   // 管理端用户名
    pub password: String,               // 密码
    pub bind: String,                   // 监听地址
    pub port: Option<u16>,              // 监听的端口
    pub mode: Option<String>,           // 启动模式， 文件配置或者高可用获取的配置
    pub server_url: Option<String>,     // 高可用地址
    pub hook_id: Option<String>,        // 高可用获取数据时使用的hook_id
    pub cluster: Option<Vec<String>>,   // 从高可用获取的集群列表
    // pub auth: bool,                     // 是否开启审计， 该配置没有使用
    pub platform: Vec<Platform>,        // 后端集群信息的配置列表
    pub user_info: Vec<UserConfig>,     // 前端可用的用户列表
}
impl MyConfig{
    /// 在启动初始化是， 如果为mp从高可用获取的模式，会获取到信息执行重制配置
    ///
    /// 所有模式为mp, 配置文件里读写节点信息配置多少都无所谓
    pub fn reset_init_config(&mut self, ha_route: &crate::server::mysql_mp::ResponseValue) {
        let mut platform_config_list = self.platform.clone();
        for cluster_route in  &ha_route.value.route{
            self.alter_platform_config(&mut platform_config_list, cluster_route);
        }
        self.platform = platform_config_list;
    }

    /// 初始化读写节点信息， 如果为mgr则不修改，使用原始配置
    fn alter_platform_config(&self, platform_config: &mut Vec<Platform>, route_info: &RouteInfo) {
        for platform in platform_config{
            if platform.platform == route_info.cluster_name && !self.check_mgr(platform){
                platform.write = Some(route_info.get_write_host_info());
                platform.read = route_info.get_read_host_info(0);
            }
        }
    }

    fn check_mgr(&self, platform_config: &Platform) -> bool{
        return if let Some(v) = &platform_config.mgr {
            v.clone()
        } else {
            false
        }
    }

    pub fn check_is_mp(&self) -> bool{
        if let Some(mode_type) = &self.mode{
            if mode_type == &String::from("mp"){
                return true;
            }
        }
        return false;
    }

    /// 获取连接池platform名称， 同时返回客户端设置的是子platform还是主platform
    ///
    /// 子paltform返回false， 用于判断
    pub fn get_pool_platform(&self, client_platform_name: &String) -> (Option<String>, bool) {
        for platform_info in &self.platform{
            if &platform_info.platform == client_platform_name{
                return (Some(platform_info.platform.clone()), false);
            }
            for client_platform in &platform_info.platform_sublist{
                if client_platform == client_platform_name{
                    return (Some(platform_info.platform.clone()), true);
                }
            }
        }
        return (None, false)
    }

    pub fn clone_config(&self) -> MyConfig{
        self.clone()
    }
}


/// 记录每个platform所属的全部配置项
#[derive(Debug, Deserialize, Clone)]
pub struct Platform {
    pub platform: String,                   // platform名称
    pub platform_sublist: Vec<String>,      // 子属性platform名称列表
    pub write: Option<String>,              // 可写节点信息
    pub read: Option<Vec<String>>,          // 可读节点列表
    pub user: String,                       // 连接池使用的用户名
    pub password: String,                   // 密码
    pub max: usize,                         // 连接池最大链接数配置
    pub min: usize,                         // 连接池最小配置
    pub mgr: Option<bool>,                  // 是否为mgr集群
    // pub auth: bool                          // 是否开启审计功能
}
impl Platform{
    pub fn get_write_host(&self) -> String{
        if let Some(v) = &self.write{
            return v.clone();
        }
        return "".to_string();
    }
}

pub fn info_now_time(t: String) -> String {
    //let dt = Local::now();
    //let now_time = dt.timestamp_millis() as usize;
    // let a = format!("{} end_time: {}", t, now_time);
    let a = format!("{}", t);
    return a;
}

pub fn get_now_time() -> usize {
    let dt = Local::now();
    let now_time = dt.timestamp_millis() as usize;
    return now_time;
}


pub fn save_class_info_time(master_info: &mut ClassUseTimeAll, sub_info: &mut ClassUseTimeAll){
    sub_info.end();
    master_info.append_sub_class(sub_info.clone());
}

#[derive(Debug)]
pub struct MyError(String);

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MyError {}

pub const DEFAULT_PORT: &str = "3306";
pub const DEFAULT_USER: &str = "root";
pub const DEFAULT_PASSWORD: &str = "root";
pub const MIN: &str = "10";
pub const MAX: &str = "20";
pub const DEFAULT_HOST_INFO: &str = "127.0.0.1:3306";
pub const MAX_CONNECTIONS: usize = 1000;
pub const VERSION: &str = "1.0.62-MysqlBus";


/// 创建数据库连接池时所需要的配置信息
#[derive(Clone, Debug)]
pub struct Config {
    // pub user: String,
    // pub password: String,
    pub muser: String,              // 链接后端db所使用的用户名
    pub mpassword: String,          // 密码
    pub program_name: String,       // 客户端名称
    pub database: String,           // 默认的数据库
    pub min: usize,                 // 创建的最小链接
    pub max: usize,                 // 最大链接
    pub host_info: String,          // 后端数据库地址
}

impl Config{
    pub fn new(platform_conf: &Platform) -> Config{
        Config{
            muser: platform_conf.user.clone(),
            mpassword: platform_conf.password.clone(),
            program_name: String::from("Tubing"),
            database: String::from("information_schema"),
            min: platform_conf.min.clone(),
            max: platform_conf.max.clone(),
            host_info: "".to_string()
        }
    }
}


pub fn read_config_from_file(config_file: &String) -> MyConfig {
    let mut file = match File::open(&config_file) {
        Ok(f) => f,
        Err(e) => panic!("no such file {} exception:{}", &config_file, e)
    };
    let mut str_val = String::new();
    match file.read_to_string(&mut str_val) {
        Ok(s) => s,
        Err(e) => panic!("Error Reading file: {}", e)
    };
    let my_config: MyConfig = toml::from_str(&str_val).unwrap();
    return my_config
}

//#[tokio::main]
fn main() -> mysql::Result<()> {
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();
    let config_file = cli.config.unwrap_or("default.toml".to_string());
    // let mut file = match File::open(&config_file) {
    //     Ok(f) => f,
    //     Err(e) => panic!("no such file {} exception:{}", &config_file, e)
    // };
    // let mut str_val = String::new();
    // match file.read_to_string(&mut str_val) {
    //     Ok(s) => s,
    //     Err(e) => panic!("Error Reading file: {}", e)
    // };
    // let my_config: MyConfig = toml::from_str(&str_val).unwrap();
    //let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?;

    let my_config = read_config_from_file(&config_file);
    server::run(my_config,config_file)
}


#[derive(StructOpt, Debug)]
#[structopt(name = "MysqlBus", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A Mysql server proxy")]
struct Cli {
    // #[structopt(name = "port", long = "--port", help="弃用")]
    // port: Option<String>,
    //
    // #[structopt(name = "username", long = "--user", help="弃用")]
    // user: Option<String>,
    //
    // #[structopt(name = "password", long = "--password", help="弃用")]
    // password: Option<String>,
    //
    // #[structopt(name = "conns", long = "--conns", help="弃用")]
    // conns: Option<String>,
    //
    // #[structopt(name = "musername", long = "--musername", help="弃用")]
    // muser: Option<String>,
    //
    // #[structopt(name = "mpassword", long = "--mpassword", help="弃用")]
    // mpassword: Option<String>,
    //
    // #[structopt(name = "mport", long = "--mport", help="弃用")]
    // mport: Option<String>,
    //
    // #[structopt(name = "min", long = "--min", help="弃用")]
    // min: Option<String>,
    //
    // #[structopt(name = "max", long = "--max", help="弃用")]
    // max: Option<String>,
    //
    // #[structopt(name = "host_info", long = "--hostinfo", help="弃用")]
    // host_info: Option<String>,

    #[structopt(name = "config", long = "--defaults-file", help="指定配置文件, 通过配置文件配置所有项")]
    config: Option<String>,
}
