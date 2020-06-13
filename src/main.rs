mod dbengine;
mod server;
mod mysql;
mod readvalue;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;
use std::sync::Arc;
use std::ops::DerefMut;
use std::thread;
use std::fmt;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;


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
    pub conns: u8,
    pub muser: String,
    pub mpassword: String,
    pub program_name: String,
    pub database: String,
    pub min: usize,
    pub max: usize,
    pub host_info: String,
}

impl Config{
    pub fn my_clone(&self) -> Config{
        Config{
            user: self.user.clone(),
            password: self.password.clone(),
            conns: self.conns.clone(),
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();
    let port = cli.port.unwrap_or(DEFAULT_PORT.to_string());
    let user = cli.user.unwrap_or(DEFAULT_USER.to_string());
    let password = cli.password.unwrap_or(DEFAULT_PASSWORD.to_string());
    let conns = cli.conns.unwrap_or(MIN.to_string()).parse().unwrap();
    let muser = cli.muser.unwrap_or(DEFAULT_USER.to_string());
    let mpassword = cli.mpassword.unwrap_or(DEFAULT_PASSWORD.to_string());
    let min = cli.min.unwrap_or(MIN.to_string()).parse().unwrap();
    let max = cli.max.unwrap_or(MAX.to_string()).parse().unwrap();
    let host_info = cli.host_info.unwrap_or(DEFAULT_HOST_INFO.to_string());

    let config = Config{user, password, conns, muser, mpassword,
        program_name: String::from("MysqlBus"),
        database: String::from("information_schema"), min, max, host_info};

    let mysql_pool = mysql::pool::ConnectionsPool::new(&config)?;

//    //节点状态检查线程
//    let mut a = mysql_pool.clone();
//    tokio::spawn(async move{
//        return a.pool_check_maintain(signal::ctrl_c()).await;
//    });
    let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?;
    let config_arc = Arc::new(config);
    server::run(listener, config_arc,  mysql_pool, signal::ctrl_c()).await

//    use sqlparser::dialect::GenericDialect;
//    use sqlparser::parser::Parser;
//    use sqlparser::ast::Statement;
//
//    let dialect = GenericDialect {}; // or AnsiDialect
//
////    let sql = "SELECT a, b, 123, myfunc(b) \
////           FROM table_1 \
////           WHERE a > b AND b < 100 \
////           ORDER BY a DESC, b";
//
//    let sql = "select * from t1 where cc = 11 limit 10 offset 100 row; update t2 set a1 = 1 where a2=3;";
//    let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();
//    for i in ast{
//        match i {
//            Statement::Query(c) => {
//                println!("{:?}", c)
//            }
//            _ => {
//                println!("{:?}", i)
//            }
//        }
//    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "MysqlBus", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A Mysql server proxy")]
struct Cli {
    #[structopt(name = "port", long = "--port")]
    port: Option<String>,

    #[structopt(name = "username", long = "--user")]
    user: Option<String>,

    #[structopt(name = "password", long = "--password")]
    password: Option<String>,

    #[structopt(name = "conns", long = "--conns")]
    conns: Option<String>,

    #[structopt(name = "musername", long = "--musername")]
    muser: Option<String>,

    #[structopt(name = "mpassword", long = "--mpassword")]
    mpassword: Option<String>,

    #[structopt(name = "mport", long = "--mport")]
    mport: Option<String>,

    #[structopt(name = "min", long = "--min")]
    min: Option<String>,

    #[structopt(name = "max", long = "--max")]
    max: Option<String>,

    #[structopt(name = "host_info", long = "--hostinfo")]
    host_info: Option<String>,
}
