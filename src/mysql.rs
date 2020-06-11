/*
@author: xiao cai niao
@datetime: 2020/5/29
*/

use crate::Result;
use std::net::{TcpStream, IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicUsize, Ordering};
pub mod connection;
use connection::MysqlConnection;
pub mod scramble;
pub mod pool;

/// mysql 协议包枚举类型
pub enum PackType {
    HandShakeResponse,
    HandShake,
    OkPacket,
    ErrPacket,
    EOFPacket,
    TextResult,
    ComQuery,
    ComQuit,
    ComInitDb,
    ComFieldList,
    ComPrefresh,
    ComStatistics,
    ComProcessInfo,
    ComProcessKill,
    ComDebug,
    ComPing,
    ComChangeUser,
    ComResetConnection,
    ComSetOption,
    ComStmtPrepare,
    ComStmtExecute,
    ComStmtFetch,
    ComStmtClose,
    ComStmtReset,
    ComStmtSendLongData,
}

pub struct FlagsMeta{
    pub multi_results: i32,
    pub secure_connection: i32,
    pub client_plugin_auth: i32,
    pub client_connect_attrs: i32,
    pub client_plugin_auth_lenenc_client_data: i32,
    pub client_deprecate_eof: i32,
    pub long_password: i32,
    pub long_flag: i32,
    pub protocol_41: i32,
    pub transactions: i32,
    pub client_connect_with_db: i32,
}

impl FlagsMeta {
    pub fn new() -> Self {
        Self {
            multi_results : 1 << 17,
            secure_connection: 1 << 15,
            client_plugin_auth: 1 << 19,
            client_connect_attrs: 1<< 20,
            client_plugin_auth_lenenc_client_data: 1<<21,
            client_deprecate_eof: 1 << 24,
            long_password: 1,
            long_flag: 1 << 2,
            protocol_41: 1 << 9,
            transactions: 1 << 13,
            client_connect_with_db: 9
        }
    }
}

/// 创建socket连接
pub fn conn(host_info: &str) -> Result<TcpStream> {
    let host_info = host_info.split(":");
    let host_vec = host_info.collect::<Vec<&str>>();
    let port = host_vec[1].to_string().parse::<u16>()?;
    let ip_vec = host_vec[0].split(".");
    let ip_vec = ip_vec.collect::<Vec<&str>>();
    let mut ip_info = vec![];
    for i in ip_vec{
        ip_info.push(i.to_string().parse::<u8>()?);
    }
    let addrs = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(ip_info[0], ip_info[1], ip_info[2], ip_info[3])), port));
    //let tcp_conn = TcpStream::connect(host_info)?;
    let tcp_conn = TcpStream::connect_timeout(&addrs, Duration::new(1,0))?;
    tcp_conn.set_read_timeout(Some(Duration::new(10,10)))?;
    tcp_conn.set_write_timeout(Some(Duration::new(10,10)))?;
    Ok(tcp_conn)
}

