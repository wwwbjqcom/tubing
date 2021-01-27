mod dbengine;
mod server;
mod mysql;
mod readvalue;
use structopt::StructOpt;
use std::fmt;

use serde_derive::{Deserialize};
use std::fs::File;
use std::io::prelude::*;
use crate::server::mysql_mp::{RouteInfo, ResponseValue};
use crate::server::mysql_mp;
use tracing::{debug};

#[derive(Debug, Deserialize, Clone)]
pub struct UserConfig{
    pub user: String,
    pub password: String,
    pub platform: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct MyConfig {
    pub user: String,
    pub password: String,
    pub bind: String,
    pub port: Option<u16>,
    pub mode: Option<String>,
    pub server_url: Option<String>,
    pub hook_id: Option<String>,
    pub cluster: Option<Vec<String>>,
    pub auth: bool,
    pub platform: Vec<Platform>,
    pub user_info: Vec<UserConfig>,
}
impl MyConfig{
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
}


#[derive(Debug, Deserialize, Clone)]
pub struct Platform {
    pub platform: String,
    pub platform_sublist: Vec<String>,
    pub write: Option<String>,
    pub read: Option<Vec<String>>,
    pub user: String,
    pub password: String,
    pub max: usize,
    pub min: usize,
    pub mgr: Option<bool>,
    pub auth: bool
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
pub const VERSION: &str = "1.0.61-MysqlBus";

#[derive(Clone, Debug)]
pub struct Config {
    // pub user: String,
    // pub password: String,
    pub muser: String,
    pub mpassword: String,
    pub program_name: String,
    pub database: String,
    pub min: usize,
    pub max: usize,
    pub host_info: String,
}

impl Config{
    pub fn new(platform_conf: &Platform) -> Config{
        Config{
            muser: platform_conf.user.clone(),
            mpassword: platform_conf.password.clone(),
            program_name: String::from("MysqlBus"),
            database: String::from("information_schema"),
            min: platform_conf.min.clone(),
            max: platform_conf.max.clone(),
            host_info: "".to_string()
        }
    }
}




//#[tokio::main]
fn main() -> mysql::Result<()> {
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();
    let config_file = cli.config.unwrap_or("default.toml".to_string());
    let mut file = match File::open(&config_file) {
        Ok(f) => f,
        Err(e) => panic!("no such file {} exception:{}", &config_file, e)
    };
    let mut str_val = String::new();
    match file.read_to_string(&mut str_val) {
        Ok(s) => s,
        Err(e) => panic!("Error Reading file: {}", e)
    };
    let mut my_config: MyConfig = toml::from_str(&str_val).unwrap();
    //let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?;
    use futures::executor::block_on;
    if my_config.check_is_mp(){
        let ha_route: ResponseValue  = block_on(mysql_mp::get_platform_route(&my_config))?;
        debug!("get_platform_route: {:?}", &ha_route);
        my_config.reset_init_config(&ha_route);
    }

    server::run(my_config)
}


#[derive(StructOpt, Debug)]
#[structopt(name = "MysqlBus", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A Mysql server proxy")]
struct Cli {
    #[structopt(name = "port", long = "--port", help="弃用")]
    port: Option<String>,

    #[structopt(name = "username", long = "--user", help="弃用")]
    user: Option<String>,

    #[structopt(name = "password", long = "--password", help="弃用")]
    password: Option<String>,

    #[structopt(name = "conns", long = "--conns", help="弃用")]
    conns: Option<String>,

    #[structopt(name = "musername", long = "--musername", help="弃用")]
    muser: Option<String>,

    #[structopt(name = "mpassword", long = "--mpassword", help="弃用")]
    mpassword: Option<String>,

    #[structopt(name = "mport", long = "--mport", help="弃用")]
    mport: Option<String>,

    #[structopt(name = "min", long = "--min", help="弃用")]
    min: Option<String>,

    #[structopt(name = "max", long = "--max", help="弃用")]
    max: Option<String>,

    #[structopt(name = "host_info", long = "--hostinfo", help="弃用")]
    host_info: Option<String>,

    #[structopt(name = "config", long = "--defaults-file", help="指定配置文件, 通过配置文件配置所有项")]
    config: Option<String>,
}
