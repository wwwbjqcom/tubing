/*
@author: xiao cai niao
@datetime: 2020/5/28
*/

use sqlparser::ast::{SetExpr, SetVariableValue};
use sqlparser::ast::Select;
use sqlparser::ast::TableFactor;
use sqlparser::ast::Statement;
use sqlparser::ast::Value;
use bytes::BytesMut;
use std::io::{Cursor, Read};
use crate::{Result, readvalue};
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use crate::dbengine::{PacketType, CLIENT_BASIC_FLAGS, CLIENT_PROTOCOL_41, CLIENT_DEPRECATE_EOF, CLIENT_SESSION_TRACK};
use crate::server::{Handler, ConnectionStatus};
use sqlparser::parser::Parser;
use sqlparser::dialect::GenericDialect;
use crate::mysql::pool::MysqlConnectionInfo;
use crate::mysql::connection::response::pack_header;
use crate::mysql::connection::PacketHeader;
use std::borrow::{BorrowMut, Borrow};

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
                let mut conn = handler.pool.get_pool(&handler.hand_key).await?;
                if let Err(e) = self.parse_query_packet(handler, &mut conn).await{
                    handler.pool.return_pool(conn).await?;
                    return Box::new(Err(e)).unwrap();
                };
                handler.pool.return_pool(conn).await?;
            }
            PacketType::ComInitDb => {
                let db = readvalue::read_string_value(&self.buf[1..]);
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
    async fn parse_query_packet(&self, handler: &mut Handler, conn_info: &mut MysqlConnectionInfo) -> Result<()> {
        let sql = readvalue::read_string_value(&self.buf[1..]);
        if self.check_is_change_db(handler, &sql).await?{
            return Ok(())
        }
        let dialect = GenericDialect {};
        let mut ast = Parser::parse_sql(&dialect, sql.to_string());
        match ast{
            Ok(mut ast) => {
                'a: for i in &mut ast{
                    match i{
                        Statement::SetVariable { local, variable, value } => {
                            let mut v = match value {
                                SetVariableValue::Ident(v) => {
                                    v.to_string().clone()
                                }
                                SetVariableValue::Literal(v) => {
                                    match v{
                                        Value::Number(i) => {
                                            i.clone()
                                        }
                                        _ => {
                                            "".to_string()
                                        }
                                    }
                                }
                            };
                            if variable.to_lowercase() == String::from("autocommit"){
                                self.set_autocommit(handler, &v).await?;
                            }else if variable.to_lowercase() == String::from("platform") {
                                handler.platform = Some(v);
                                self.send_ok_packet(handler).await?;
                            }else {
                                let error = String::from("only supports set autocommit/platform");
                                self.send_error_packet(handler, &error).await?;
                            }
                        }
                        Statement::Query(v) => {
                            self.exec_query(handler, conn_info).await?;
                        }
                        Statement::Commit { chain } => {
                            self.send_one_packet(handler, conn_info).await?;
                            self.reset_conn_db_and_autocommit(conn_info).await?;
                            conn_info.reset_cached().await?;
                        }
                        Statement::Insert { table_name, columns, source } => {
                            if let Err(e) = self.set_conn_db_and_autocommit(handler, conn_info).await{
                                self.send_error_packet(handler, &e.to_string()).await?;
                                return Ok(())
                            };
                            self.send_one_packet(handler, conn_info).await?;
                            self.check_auto_commit_set(handler, conn_info).await?;
                        }
                        Statement::Delete { table_name, selection } => {
                            if let Err(e) = self.set_conn_db_and_autocommit(handler, conn_info).await{
                                self.send_error_packet(handler, &e.to_string()).await?;
                                return Ok(())
                            };
                            self.send_one_packet(handler, conn_info).await?;
                            self.check_auto_commit_set(handler, conn_info).await?;
                        }
                        Statement::Update { table_name, assignments, selection } => {
                            if let Err(e) = self.set_conn_db_and_autocommit(handler, conn_info).await{
                                self.send_error_packet(handler, &e.to_string()).await?;
                                return Ok(())
                            };
                            self.send_one_packet(handler, conn_info).await?;
                            self.check_auto_commit_set(handler, conn_info).await?;
                        }
                        Statement::Rollback { chain } => {
                            self.send_one_packet(handler, conn_info).await?;
                            self.reset_conn_db_and_autocommit(conn_info).await?;
                            conn_info.reset_cached().await?;
                        }
                        Statement::StartTransaction { modes } => {
                            if let Err(e) = self.set_conn_db_and_autocommit(handler, conn_info).await{
                                self.send_error_packet(handler, &e.to_string()).await?;
                                return Ok(())
                            };
                            self.send_one_packet(handler, conn_info).await?;
                            conn_info.set_cached(&handler.hand_key);
                        }
                        Statement::AlterTable { name, operation } => {
                            self.no_traction(handler, conn_info).await?;
                        }
                        Statement::CreateTable { name, columns, constraints, with_options, external, file_format, location } => {
                            self.no_traction(handler, conn_info).await?;
                        }
                        Statement::Drop { object_type, if_exists, names, cascade } => {
                            self.no_traction(handler, conn_info).await?;
                        }
                        Statement::ShowVariable { variable } => {
                            self.exec_query(handler,conn_info).await?;
                        }
                        Statement::ShowColumns { extended, full, table_name, filter } => {
                            self.exec_query(handler,conn_info).await?;
                        }
                        _ => {
                            self.send_ok_packet(handler).await?;
                        }
                    }
                }
            }
            Err(e) => {
                if self.check_is_set_names(&sql).await?{
                    self.send_ok_packet(handler).await?;
                    //self.send_one_packet(handler, conn_info).await?;
                }else if self.check_is_show(&sql).await? {
                    self.exec_query(handler, conn_info).await?;
                }else {
                    self.send_error_packet(handler, &e.to_string()).await?;
                }

            }
        }

        Ok(())
    }

    /// 检查是否为select 语句，测试用
    async fn check_is_select(&self, handler: &mut Handler, sql: &String,conn_info: &mut MysqlConnectionInfo) -> Result<bool> {
        if sql.to_lowercase().starts_with("select") {
            self.exec_query(handler, conn_info).await?;
            return Ok(true)
        }
        return Ok(false)
    }

    /// 处理查询的连接
    async fn exec_query(&self, handler: &mut Handler, conn_info: &mut MysqlConnectionInfo) -> Result<()> {
        if let Err(e) = self.set_conn_db_and_autocommit(handler, conn_info).await{
            self.send_error_packet(handler, &e.to_string()).await?;
            return Ok(())
        };
        let packet = self.packet_my_value();
        let (buf, header) = conn_info.send_packet(&packet).await?;
        self.send_mysql_response_packet(handler, &buf, &header).await?;
        let mut eof_num = 0;
        'b: loop {
            if eof_num > 1{
                break 'b;
            }
            let (buf, mut header) = conn_info.get_packet_from_stream().await?;
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
        if conn_info.cached == "".to_string() {
            self.reset_conn_db_and_autocommit(conn_info).await?;
        }
        Ok(())
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
    async fn no_traction(&self, handler: &mut Handler, conn: &mut MysqlConnectionInfo) -> Result<()> {
        if let Err(e) = self.set_conn_db_and_autocommit(handler, conn).await{
            self.send_error_packet(handler, &e.to_string()).await?;
            return Ok(())
        };
        self.send_one_packet(handler, conn).await?;
        self.reset_conn_db_and_autocommit(conn).await?;
        Ok(())
    }

    /// 发送只回复ok/error的数据包
    async fn send_one_packet(&self, handler: &mut Handler, conn: &mut MysqlConnectionInfo) -> Result<()>{
        let packet = self.packet_my_value();
        let (buf, header) = conn.send_packet(&packet).await?;
        self.send_mysql_response_packet(handler, &buf, &header).await?;
        //self.check_eof(handler, conn).await?;
        Ok(())
    }

    /// 检查ok包是否已eof结尾
    async fn check_eof(&self, handler: &mut Handler, conn: &mut MysqlConnectionInfo) -> Result<()> {
        if handler.client_flags & CLIENT_SESSION_TRACK as i32 <= 0 {
            let (buf, header) = conn.get_packet_from_stream().await?;
            self.send_mysql_response_packet(handler, &buf, &header).await?;
        }
        Ok(())
    }

    /// 用于检查是否为自动提交， 判断是否缓存线程
    async fn check_auto_commit_set(&self, handler: &mut Handler, conn: &mut MysqlConnectionInfo) -> Result<()>{
        if handler.auto_commit{
            self.reset_conn_db_and_autocommit(conn).await?;
        }else {
            conn.set_cached(&handler.hand_key).await?;
        }
        Ok(())
    }

    async fn send_mysql_response_packet(&self, handler: &mut Handler, buf: &Vec<u8>, header: &PacketHeader) -> Result<()> {
        let my_buf = self.packet_response_value(buf, header);
        handler.send_full(&my_buf).await?;
        Ok(())
    }

    async fn set_autocommit(&self, hanler: &mut Handler, value: &String) -> Result<()>{
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
        self.send_ok_packet(hanler).await?;
        Ok(())
    }

    /// 初始化连接状态
    async fn set_conn_db_and_autocommit(&self, handler: &mut Handler, conn: &mut MysqlConnectionInfo) -> Result<()>{
        if conn.cached != String::from(""){
            return Ok(())
        }
        let mut packet = vec![];
        packet.push(3 as u8);
        if let Some(db) = &handler.db{
            if handler.auto_commit{
                self.__set_autocommit(1, conn).await?;
            }
            self.__set_default_db(db, conn).await?;
        }else {
            if handler.auto_commit{
                self.__set_autocommit(1, conn).await?;
            }
        }
        Ok(())
    }

    async fn __set_default_db(&self, db: &String, conn: &mut MysqlConnectionInfo) -> Result<()> {
        let mut packet = vec![];
        packet.push(3 as u8);
        let sql = format!("use {};", db.clone());
        packet.extend(sql.as_bytes());
        self.__set_packet_send(&packet, conn).await?;
        Ok(())
    }

    async fn __set_autocommit(&self, autocommit: u8, conn: &mut MysqlConnectionInfo) -> Result<()> {
        let mut packet = vec![];
        packet.push(3 as u8);
        let sql = format!("set autocommit={};", autocommit);
        packet.extend(sql.as_bytes());
        self.__set_packet_send(&packet, conn).await?;
        Ok(())
    }

    async fn __set_packet_send(&self, packet: &Vec<u8>, conn: &mut MysqlConnectionInfo) -> Result<()>{
        let mut packet_full = vec![];
        packet_full.extend(pack_header(&packet, 0));
        packet_full.extend(packet);
        let (buf, header) = conn.send_packet(&packet_full).await?;
        self.check_packet_is(&buf).await?;
        Ok(())
    }

    /// 恢复连接为初始化状态
    async fn reset_conn_db_and_autocommit(&self, conn: &mut MysqlConnectionInfo) -> Result<()>{
        self.__set_autocommit(0, conn).await?;
        self.__set_default_db(&String::from("information_schema"), conn).await?;
        Ok(())
    }

    async fn check_packet_is(&self, buf: &Vec<u8>) -> Result<()>{
        if buf[0] == 0xff {
            let error = readvalue::read_string_value(&buf[3..]);
            return Box::new(Err(error)).unwrap();
        }
        Ok(())
    }

    /// 检查是否为use db语句
    ///
    /// 因为sqlparse不支持该类语句
    async fn check_is_change_db(&self, handler: &mut Handler, sql: &String) -> Result<bool>{
        if sql.to_lowercase().starts_with("use"){
            let sql = sql.to_lowercase();
            let sql_ver = sql.split(" ");
            let sql_ver = sql_ver.collect::<Vec<&str>>();
            let mut tmp: Vec<String> = vec![];
            for i in &sql_ver{
                if &i.to_string() != &"".to_string(){
                    tmp.push(i.to_string().clone())
                }
            }
            handler.db = Some(tmp[1].to_string().clone());
            self.send_ok_packet(handler).await?;
            return Ok(true)
        }
        Ok(false)
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
