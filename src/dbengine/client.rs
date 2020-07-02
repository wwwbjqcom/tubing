/*
@author: xiao cai niao
@datetime: 2020/5/28
*/

use bytes::{BytesMut, Buf};
use std::io::{Cursor, Read};
use crate::{Result, readvalue};
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use crate::dbengine::{PacketType, CLIENT_BASIC_FLAGS, CLIENT_PROTOCOL_41, CLIENT_DEPRECATE_EOF, CLIENT_SESSION_TRACK};
use crate::server::{Handler, ConnectionStatus};
use crate::mysql::pool::{MysqlConnectionInfo, ConnectionsPool};
use crate::mysql::connection::response::pack_header;
use crate::mysql::connection::PacketHeader;
use std::borrow::{BorrowMut, Borrow};
use tracing_subscriber::util::SubscriberInitExt;
use tracing::{error, debug, info, instrument};
use crate::MyError;
use crate::server::sql_parser::SqlStatement;
use std::net::TcpStream;
use std::time::Duration;
use tokio::time::delay_for;
use tracing::field::debug;

#[derive(Debug)]
pub struct ClientResponse {
    pub payload: u32,
    pub seq: u8,
    pub buf: Vec<u8>
}

impl ClientResponse {
    pub async fn new(packet: &mut Cursor<&[u8]>) -> Result<ClientResponse> {
        let payload = packet.read_u24::<LittleEndian>()?;
        let seq = packet.read_u8()?;
        let mut buf = vec![0 as u8; payload as usize];
        packet.read_exact(&mut buf)?;
        Ok(ClientResponse{
            payload,
            seq,
            buf
        })
    }

    /// 解析客户端发送的请求类型，并处理请求
    pub async fn exec(&self, handler: &mut Handler) -> Result<()> {
        match PacketType::new(&self.buf[0]){
            PacketType::ComQuit => {
                handler.status = ConnectionStatus::Quit;
            }
            PacketType::ComPing => {
                self.send_ok_packet(handler).await?;
            }
            PacketType::ComQuery => {
                debug!("{}",crate::info_now_time(String::from("start check pool info")));
                handler.per_conn_info.check(&mut handler.pool, &handler.hand_key, &handler.db, &handler.auto_commit).await?;
                debug!("{}",crate::info_now_time(String::from("start parse query packet")));
                if let Err(e) = self.parse_query_packet(handler).await{
                    return Err(Box::new(MyError(e.to_string().into())));
                };
            }
            PacketType::ComInitDb => {
                let db = readvalue::read_string_value(&self.buf[1..]);
                debug(format!("initdb {}", &db));
                handler.db = Some(db);
                self.send_ok_packet(handler).await?;
            }
            _ => {
                let err = String::from("Invalid packet type, only supports com_query/com_quit");
                self.send_error_packet(handler, &err).await?;
            }
        }
        Ok(())
    }

    /// 处理com_query packet
    ///
    /// 解析处sql语句
    ///
    /// 如果为set本地执行修改并回复
    ///
    /// 如果为dml语句根据handler记录的auto_commit/db信息执行对应连接的状态修改和sql语句修改
    /// 完毕后直接发送到后端mysql执行，并直接用mysql的回包回复客户端
    ///
    /// 如果为use语句，直接修改hanler中db的信息，并回复
    async fn parse_query_packet(&self, handler: &mut Handler) -> Result<()> {
        let sql = readvalue::read_string_value(&self.buf[1..]);
        let sql_parser = SqlStatement::Default;
//        if self.check_is_change_db(handler, &sql).await?{
//            handler.set_per_conn_cached().await?;
//            return Ok(())
//        }
//        let dialect = MySqlDialect {};
//        let mut ast = Parser::parse_sql(&dialect, sql.to_string());
//        match ast{
//            Ok(mut ast) => {
//                'a: for i in &mut ast{
        let a = sql_parser.parser(&sql);
        debug!("{}",crate::info_now_time(String::from("parser sql sucess")));
        debug!("{}",format!("{:?}: {}", a, &sql));
        match a{
            SqlStatement::ChangeDatabase => {
                self.check_is_change_db(handler, &sql).await?;
                //handler.set_per_conn_cached().await?;
            }
            SqlStatement::SetVariable (variable, value) => {
                if variable.to_lowercase() == String::from("autocommit"){
                    self.set_autocommit(handler, &value).await?;
                }else if variable.to_lowercase() == String::from("platform") {
                    handler.platform = Some(value);
                    self.send_ok_packet(handler).await?;
                }else if variable.to_lowercase() == String::from("names"){
                    self.send_ok_packet(handler).await?;
                }else {
                    let error = String::from("only supports set autocommit/platform/names");
                    error!("{}", &error);
                    self.send_error_packet(handler, &error).await?;
                }
            }
            SqlStatement::Query => {
                self.exec_query(handler).await?;
            }
            SqlStatement::Commit => {
                self.send_one_packet(handler).await?;
                //self.reset_conn_db_and_autocommit(handler, conn_info)?;
                self.reset_is_transaction(handler).await?;
            }
            SqlStatement::Insert => {
//                if let Err(e) = self.set_conn_db_and_autocommit(handler).await{
//                    self.send_error_packet(handler, &e.to_string()).await?;
//                    return Ok(())
//                };
                self.send_one_packet(handler).await?;
                self.check_is_no_autocommit(handler).await?;
                //self.check_auto_commit_set(handler, conn_info).await?;
            }
            SqlStatement::Delete => {
//                if let Err(e) = self.set_conn_db_and_autocommit(handler).await{
//                    self.send_error_packet(handler, &e.to_string()).await?;
//                    return Ok(())
//                };
                self.send_one_packet(handler).await?;
                self.check_is_no_autocommit(handler).await?;
                //self.check_auto_commit_set(handler, conn_info).await?;
            }
            SqlStatement::Update => {
//                if let Err(e) = self.set_conn_db_and_autocommit(handler).await{
//                    self.send_error_packet(handler, &e.to_string()).await?;
//                    return Ok(())
//                };
                self.send_one_packet(handler).await?;
                self.check_is_no_autocommit(handler).await?;
                //self.check_auto_commit_set(handler, conn_info).await?;
            }
            SqlStatement::Rollback => {
                self.send_one_packet(handler).await?;
                self.reset_is_transaction(handler).await?;
                //self.reset_conn_db_and_autocommit(handler, conn_info)?;
                //conn_info.reset_cached().await?;
            }
            SqlStatement::StartTransaction  => {
//                if let Err(e) = self.set_conn_db_and_autocommit(handler).await{
//                    self.send_error_packet(handler, &e.to_string()).await?;
//                    return Ok(())
//                };
                self.send_one_packet(handler).await?;
                self.set_is_transaction(handler).await?;
            }
            SqlStatement::AlterTable => {
                self.no_traction(handler).await?;
            }
            SqlStatement::Create => {
                self.no_traction(handler).await?;
            }
            SqlStatement::Drop => {
                self.no_traction(handler).await?;
            }
            SqlStatement::Show => {
                self.exec_query(handler).await?;
            }
            SqlStatement::Default => {
                let error = String::from("Unsupported syntax");
                error!("{}",&error);
                self.send_error_packet(handler, &error).await?;
            }
        }

        Ok(())
    }

    async fn reset_is_transaction(&self, handler: &mut Handler) -> Result<()> {
        if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
            return Ok(conn_info.reset_is_transaction().await?);
        }
        return Err(Box::new(MyError(String::from("lost connection").into())));
    }

    async fn set_is_transaction(&self, handler: &mut Handler) -> Result<()> {
        if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
            return Ok(conn_info.set_is_transaction().await?);
        }
        return Err(Box::new(MyError(String::from("lost connection").into())));
    }

    async fn set_cached(&self, handler: &mut Handler) -> Result<()>{
        let hand_key = handler.hand_key.clone();
        if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
            return Ok(conn_info.set_cached(&hand_key).await?);
        }
        return Err(Box::new(MyError(String::from("lost connection").into())));
    }

    /// 检查是否为非自动提交， 用于数据变动时设置连接状态
    async fn check_is_no_autocommit(&self, handler: &mut Handler) -> Result<()>{
        if !handler.auto_commit{
            if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
                conn_info.set_is_transaction().await?;
            }
        }
        Ok(())
    }

    /// 检查是否为select 语句，测试用
    async fn check_is_select(&self, handler: &mut Handler, sql: &String) -> Result<bool> {
        if sql.to_lowercase().starts_with("select") {
            self.exec_query(handler).await?;
            return Ok(true)
        }
        return Ok(false)
    }

    /// 处理查询的连接
    async fn exec_query(&self, handler: &mut Handler) -> Result<()> {
        debug!("{}",crate::info_now_time(String::from("start execute query")));
//        if let Err(e) = self.set_conn_db_for_query(handler).await{
//            //handler.send_error_packet(&e.to_string()).await?;
//            self.send_error_packet(handler, &e.to_string()).await?;
//            return Ok(())
//        };
        debug!("{}",crate::info_now_time(String::from("set conn db for query sucess")));
        let packet = self.packet_my_value();
        debug!("{}",crate::info_now_time(String::from("start send pakcet to mysql")));
        let (buf, header) = self.send_packet(handler, &packet).await?;
        debug!("{}",crate::info_now_time(String::from("start and mysql response to client")));
        self.send_mysql_response_packet(handler, &buf, &header).await?;
        debug!("{}",crate::info_now_time(String::from("send_mysql_response_packet sucess")));
        //如果发生错误直接退出， 如果不是将继续接收数据包，因为只有错误包只有一个，其他数据都会是连续的
        if buf[0] == 0xff{
            return Ok(());
        }
        let mut eof_num = 0;
        'b: loop {
            if eof_num > 1{
                break 'b;
            }
            let (buf, mut header) = self.get_packet_from_stream(handler).await?;
            if buf[0] == 0xff {
                self.send_mysql_response_packet(handler, &buf, &header).await?;
                break 'b;
            }else{
                let (is_eof,num) = self.check_p(&buf, eof_num.clone(), &header);
                if is_eof{
                    eof_num = num;
                }
                self.query_response(handler, &buf, &mut header, &eof_num, is_eof).await?;
            }

        }
        debug!("{}",crate::info_now_time(String::from("send ok")));
        Ok(())
    }

    async fn get_packet_from_stream(&self, handler: &mut Handler) -> Result<(Vec<u8>, PacketHeader)>{
        if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
            return Ok(conn_info.get_packet_from_stream().await?);
        }
        let error = String::from("lost connection");
        return Err(Box::new(MyError(error.into())));
    }

    async fn send_packet(&self, handler: &mut Handler, packet: &Vec<u8>) -> Result<(Vec<u8>, PacketHeader)>{
        if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
            return Ok(conn_info.send_packet(&packet).await?);
        }
        let error = String::from("lost connection");
        return Err(Box::new(MyError(error.into())));
    }

    async fn query_response(&self, handler: &mut Handler, buf: &Vec<u8>, header: &mut PacketHeader, eof_num: &i32, is_eof: bool) -> Result<()> {
        if handler.client_flags & CLIENT_DEPRECATE_EOF as i32 > 0{
            //客户端没有metadata eof结束包， 这里需要对metadata eof后续的包进行改写seq_id
            if eof_num == 1.borrow() && !is_eof {
                // metadata eof之后的包需要改写seq_id
                header.seq_id = header.seq_id -1;
                self.send_mysql_response_packet(handler, &buf, &header).await?;
            }else if eof_num == 0.borrow() {
                // metadata eof之前的数据包直接发送
                self.send_mysql_response_packet(handler, &buf, &header).await?;
            }else if eof_num > 1.borrow() && is_eof {
                header.seq_id = header.seq_id -1;
                self.send_mysql_response_packet(handler, &buf, &header).await?;
            }
        }else {
            self.send_mysql_response_packet(handler, &buf, &header).await?;
        }
        Ok(())
    }

    fn check_p(&self, buf: &Vec<u8>, eof_num: i32, header: &PacketHeader) -> (bool, i32) {
        if buf[0] == 0x00{
            if header.payload < 9{
                return (true, eof_num + 1);
            }
        }else if buf[0] == 0xfe {
            return (true, eof_num + 1);
        }
        return (false, eof_num);
    }

    /// 用于非事务性的操作
    async fn no_traction(&self, handler: &mut Handler) -> Result<()> {
        if let Some(conn) = &mut handler.per_conn_info.conn_info{
//            if let Err(e) = self.set_conn_db_and_autocommit(handler).await{
//                self.send_error_packet(handler, &e.to_string()).await?;
//                return Ok(())
//            };
            self.send_one_packet(handler).await?;
        }
        Ok(())
    }

    /// 发送只回复ok/error的数据包
    async fn send_one_packet(&self, handler: &mut Handler) -> Result<()>{
        if let Some(conn) = &mut handler.per_conn_info.conn_info{
            let packet = self.packet_my_value();
            let (buf, header) = conn.send_packet(&packet).await?;
            self.send_mysql_response_packet(handler, &buf, &header).await?;
            //self.check_eof(handler, conn).await?;
        }
        Ok(())
    }

    /// 检查ok包是否已eof结尾
    async fn check_eof(&self, handler: &mut Handler) -> Result<()> {
        if let Some(conn) = &mut handler.per_conn_info.conn_info{
            if handler.client_flags & CLIENT_SESSION_TRACK as i32 <= 0 {
                let (buf, header) = conn.get_packet_from_stream().await?;
                self.send_mysql_response_packet(handler, &buf, &header).await?;
            }
        }
        Ok(())
    }

//    /// 用于检查是否为自动提交， 判断是否缓存线程
//    async fn check_auto_commit_set(&self, handler: &mut Handler) -> Result<()>{
//        if let Some(conn) = &mut handler.per_conn_info.conn_info{
//            if handler.auto_commit{
//                self.reset_conn_db_and_autocommit(handler, conn)?;
//            }else {
//                conn.set_cached(&handler.hand_key).await?;
//            }
//        }
//        Ok(())
//    }

    async fn send_mysql_response_packet(&self, handler: &mut Handler, buf: &Vec<u8>, header: &PacketHeader) -> Result<()> {
        debug!("{}",crate::info_now_time(String::from("start send packet to mysql")));
        let my_buf = self.packet_response_value(buf, header);
        handler.send_full(&my_buf).await?;
        Ok(())
    }

    async fn set_autocommit(&self, hanler: &mut Handler, value: &String) -> Result<()>{
        let tmp = hanler.auto_commit.clone();
        if value.to_lowercase() == String::from("true"){
            hanler.auto_commit = true;
        }else if value.to_lowercase() == String::from("false") {
            hanler.auto_commit = false;
        }else if value.to_lowercase() == String::from("0") {
            hanler.auto_commit = false;
        }else if value.to_lowercase() == String::from("1") {
            hanler.auto_commit = true;
        }else {
            let error = String::from("autocommit value only supports 0/1/true/false");
            self.send_error_packet(hanler, &error).await?;
            return Ok(())
        }
        if let Some(conn_info) = &mut hanler.per_conn_info.conn_info{
            if tmp != hanler.auto_commit && conn_info.hash_cache(&hanler.hand_key){
                if hanler.auto_commit{
                    self.__set_autocommit(1, hanler).await?;
                }else {
                    self.__set_autocommit(0, hanler).await?;
                }
            }
        }
        self.send_ok_packet(hanler).await?;
        Ok(())
    }

//    /// 判断并初始化默认库，只针对查询/show语句
//    async fn set_conn_db_for_query(&self, handler: &mut Handler) -> Result<()>{
//        if let Some(conn) = &mut handler.per_conn_info.conn_info{
//            if conn.cached != String::from(""){
//                return Ok(())
//            }
//            if let Some(db) = &handler.db{
//                if db != &String::from("information_schema"){
//                    self.__set_default_db(db.clone(), handler).await?;
//                }
//            }
//            return Ok(())
//        }
//        return Err(Box::new(MyError(String::from("lost connection").into())));
//    }

    /// 初始化连接状态
    async fn set_conn_db_and_autocommit(&self, handler: &mut Handler) -> Result<()>{
        if self.check_is_cached(handler)? {
            return Ok(());
        }
        let mut packet = vec![];
        packet.push(3 as u8);
        let my_db = handler.db.clone();
        if let Some(db) = &my_db{
            if handler.auto_commit{
                self.__set_autocommit(1, handler).await?;
            }
            if db != &String::from("information_schema"){
                self.__set_default_db(db.clone(), handler).await?;
            }
        }else {
            if handler.auto_commit{
                self.__set_autocommit(1, handler).await?;
            }
        }

        Ok(())
    }

    fn check_is_cached(&self, handler: &mut Handler) -> Result<bool> {
        if let Some(conn) = &mut handler.per_conn_info.conn_info{
            if conn.cached != String::from("") {
                return Ok(true);
            }
            return Ok(false);
        }
        return Err(Box::new(MyError(String::from("lost connection").into())));
    }

    async fn __set_default_db(&self, db: String, handler: &mut Handler) -> Result<()> {
        let mut packet = vec![];
        packet.push(3 as u8);
        let sql = format!("use {};", db);
        packet.extend(sql.as_bytes());
        self.__set_packet_send(&packet, handler).await?;
        Ok(())
    }

    async fn __set_autocommit(&self, autocommit: u8, handler: &mut Handler) -> Result<()> {
        let mut packet = vec![];
        packet.push(3 as u8);
        let sql = format!("set autocommit={};", autocommit);
        packet.extend(sql.as_bytes());
        self.__set_packet_send(&packet, handler).await?;
        Ok(())
    }

    async fn __set_packet_send(&self, packet: &Vec<u8>, handler: &mut Handler) -> Result<()>{
        let mut packet_full = vec![];
        packet_full.extend(pack_header(&packet, 0));
        packet_full.extend(packet);
        if let Some(conn) = &mut handler.per_conn_info.conn_info{
            let (buf, header) = conn.send_packet(&packet_full).await?;
            conn.check_packet_is(&buf)?;
            return Ok(());
        }
        return Err(Box::new(MyError(String::from("lost connection").into())));
    }

//    /// 恢复连接为初始化状态
//    fn reset_conn_db_and_autocommit(&self, handler: &mut Handler, conn: &mut MysqlConnectionInfo) -> Result<()>{
//        //如果当前连接为自动提交则初始化设置，默认是不自动提交
//        if handler.auto_commit{
//            self.__set_autocommit(0, handler)?;
//        }
//        //如果当前连接设置的db不为information_schema才初始化db信息
//        if let Some(db) = &handler.db{
//            if db != &String::from("information_schema"){
//                self.__set_default_db(&String::from("information_schema"), conn)?;
//            }
//        }
//        Ok(())
//    }

    /// 检查是否为use db语句
    ///
    /// 因为sqlparse不支持该类语句
    async fn check_is_change_db(&self, handler: &mut Handler, sql: &String) -> Result<()>{
//        if sql.to_lowercase().starts_with("use"){
        let sql = sql.to_lowercase();
        let sql_ver = sql.split(" ");
        let sql_ver = sql_ver.collect::<Vec<&str>>();
        let mut tmp: Vec<String> = vec![];
        for i in &sql_ver{
            if &i.to_string() != &"".to_string(){
                tmp.push(i.to_string().clone())
            }
        }
        let my_tmp = tmp[1].to_string().clone();
        if let Err(e) = self.__set_default_db(my_tmp.clone(), handler).await{
            self.send_error_packet(handler, &e.to_string()).await?;
        }else {
            handler.db = Some(my_tmp);
            self.send_ok_packet(handler).await?;
        }
        return Ok(())
//        }
//        Ok(false)
    }

    /// 检查是否为set 语句
    ///
    /// 因为不需要发送到后端，直接返回OK
    async fn check_is_set_names(&self,  sql: &String) -> Result<bool> {
        if sql.to_lowercase().starts_with("set"){
            let sql = sql.to_lowercase();
            let sql_ver = sql.split(" ");
            let sql_ver = sql_ver.collect::<Vec<&str>>();
            let mut tmp: Vec<String> = vec![];
            for i in &sql_ver{
                if &i.to_string() != &"".to_string(){
                    tmp.push(i.to_string().clone())
                }
            }
            if tmp[1].to_string().to_lowercase() == "names".to_string(){
                return Ok(true);
            }
        }
        return Ok(false)
    }


    /// 检查是否为show 语句
    ///
    /// 部分show 语句parse无法支持
    async fn check_is_show(&self, sql: &String) -> Result<bool> {
        if sql.to_lowercase().starts_with("show") {
            return Ok(true)
        }
        return Ok(false)
    }

    /// 发送ok packet
    ///
    /// 因为update/delete/insert会直接发送到mysql， 也是使用mysql的包做回复
    /// 所以这里只做了status flags的操作
    async fn send_ok_packet(&self, handler: &mut Handler) -> Result<()>{
        let status_flags = handler.get_status_flags();
        let mut packet = vec![];
        packet.push(0); //packet type
        packet.push(0); //affected_rows
        packet.push(0); //last_insert_id
        if CLIENT_BASIC_FLAGS & CLIENT_PROTOCOL_41 > 0{
            packet.extend(readvalue::write_u16(status_flags));
            packet.extend(vec![0,0]);    //warnings
        }
        handler.send(&packet).await?;
        handler.reset_seq();
        Ok(())
    }

    /// 发送error packet
    async fn send_error_packet(&self, handler: &mut Handler, error: &String) -> Result<()>{
        let mut err = vec![];
        err.push(0xff);
        err.extend(readvalue::write_u16(10));
        if CLIENT_BASIC_FLAGS & CLIENT_PROTOCOL_41 > 0{
            for _ in 0..5{
                err.push(0);
            }
        }
        err.extend(error.as_bytes());
        handler.send(&err).await?;
        handler.reset_seq();
        Ok(())
    }


    fn packet_my_value(&self) -> Vec<u8>{
        let mut buf = vec![];
        buf.extend(readvalue::write_u24(self.payload.clone()));
        buf.push(self.seq.clone());
        buf.extend(self.buf.clone());
        return buf;
    }

    fn packet_response_value(&self, buf: &Vec<u8>, header: &PacketHeader) -> Vec<u8> {
        let mut my_buf = vec![];
        my_buf.extend(readvalue::write_u24(header.payload.clone()));
        my_buf.push(header.seq_id.clone());
        my_buf.extend(buf.clone());
        return my_buf;
    }
}
