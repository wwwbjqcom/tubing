mod dbengine;
mod server;
mod mysql;
mod readvalue;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::runtime::{Runtime, Builder};
use tokio::prelude::*;
use std::sync::Arc;
use std::ops::DerefMut;
use std::thread;
use std::fmt;
use tracing::debug;
use chrono::prelude::*;
use chrono;
use reqwest;

use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use std::fs::File;
use std::io::prelude::*;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
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
}

#[derive(Debug, Deserialize)]
pub struct Platform {
    pub platform: String,
    pub write: String,
    pub read: Option<Vec<String>>,
    pub user: String,
    pub password: String,
    pub clipassword: String,
    pub max: usize,
    pub min: usize,
    pub auth: bool
}

pub fn info_now_time(t: String) -> String {
    let dt = Local::now();
    let now_time = dt.timestamp_millis() as usize;
    let a = format!("{} end_time: {}", t, now_time);
    return a;
}

#[derive(Debug)]
pub struct MyError(String);

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "There is an error: {}", self.0)
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

#[derive(Clone, Debug)]
pub struct Config {
    pub user: String,
    pub password: String,
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
            user: platform_conf.user.clone(),
            password: platform_conf.clipassword.clone(),
            muser: platform_conf.user.clone(),
            mpassword: platform_conf.password.clone(),
            program_name: String::from("MysqlBus"),
            database: String::from("information_schema"),
            min: platform_conf.min.clone(),
            max: platform_conf.max.clone(),
            host_info: "".to_string()
        }
    }

    pub fn my_clone(&self) -> Config{
        Config{
            user: self.user.clone(),
            password: self.password.clone(),
            muser: self.muser.clone(),
            mpassword: self.password.clone(),
            program_name: self.program_name.clone(),
            database: self.database.clone(),
            min: self.min.clone(),
            max: self.max.clone(),
            host_info: self.host_info.clone()
        }
    }
}

#[derive(Serialize)]
pub struct GetRouteInfo {
    pub hook_id: String,
    pub clusters: Vec<String>,
}
impl GetRouteInfo{
    fn new(conf: &MyConfig) -> mysql::Result<GetRouteInfo>{
        let mut tmp = GetRouteInfo{ hook_id: "".to_string(), clusters: vec![] };
        if let Some(hook_id) = &conf.hook_id{
            tmp.hook_id = hook_id.clone();
            match &conf.cluster{
                Some(v) => {
                    if v.len()> 0{
                        tmp.clusters = v.clone();
                    }
                    tmp.clusters = vec![String::from("all")];
                }
                None => {
                    tmp.clusters = vec![String::from("all")];
                }
            }
            return Ok(tmp);
        }
        let err = String::from("hook id can not be empty");
        return Err(Box::new(MyError(err.into())));
    }
}

async fn get_platform_route(conf: &MyConfig) -> mysql::Result<()> {
    let map = json!(GetRouteInfo::new(conf)?);
    let client = reqwest::Client::new();
    if let Some(url) = &conf.server_url{
        let res = client.post(url)
            .json(&map)
            .send()
            .await?;
        println!("{:?}", &res);
    }
    Ok(())
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
        Ok(s) => s
        ,
        Err(e) => panic!("Error Reading file: {}", e)
    };
    let my_config: MyConfig = toml::from_str(&str_val).unwrap();

    let platform_pool = mysql::pool::PlatformPool::new(&my_config)?;
    //let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?;
    server::run(&my_config, signal::ctrl_c(), platform_pool)
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
