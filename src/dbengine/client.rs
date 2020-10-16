/*
@author: xiao cai niao
@datetime: 2020/5/28
*/

use std::io::{Cursor, Read};
use crate::{ readvalue};
use crate::mysql::Result;
use byteorder::{ReadBytesExt, LittleEndian};
use crate::dbengine::{PacketType, CLIENT_BASIC_FLAGS, CLIENT_PROTOCOL_41, CLIENT_DEPRECATE_EOF};
use crate::server::{Handler, ConnectionStatus};
use crate::mysql::connection::response::pack_header;
use crate::mysql::connection::PacketHeader;
use std::borrow::{Borrow};
use tracing::{error, debug, info};
use crate::MyError;
use crate::server::sql_parser::SqlStatement;
use tracing::field::{debug};
use crate::dbengine::admin::AdminSql;
use crate::mysql::query_response::TextResponse;
use crate::mysql::privileges::{CheckPrivileges, TableInfo};
use bytes::Buf;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::*;
use sqlparser::ast::Statement;

#[derive(Debug)]
pub struct ClientResponse {
    pub payload: u32,
    pub seq: u8,
    pub buf: Vec<u8>,
    pub larger: Vec<ClientResponse>         //超出16m部分
}

impl ClientResponse {
    pub async fn new(packet: &mut Cursor<&[u8]>) -> Result<ClientResponse> {

        /// 从buffer中解析packet header部分并读取payload
        ///
        /// 这里判断buffer剩余部分时候满足payload大小，如果小于payload则返回空
        fn read_buf(packet: &mut Cursor<&[u8]>) -> Result<ClientResponse>{
            let packet_len = packet.remaining();
            let payload = packet.read_u24::<LittleEndian>()?;
            let seq = packet.read_u8()?;
            let mut buf = vec![0 as u8; payload as usize];
            debug!("read_buf one packet: payload: {}, seq: {} , cursor_len: {}", &payload, &seq, &packet_len);
            return if payload <= (packet_len - 4) as u32 {
                packet.read_exact(&mut buf)?;
                Ok(ClientResponse{
                    payload,
                    seq,
                    buf,
                    larger: vec![]
                })
            }else {
                Ok(ClientResponse{
                    payload: 0,
                    seq: 0,
                    buf: vec![],
                    larger: vec![]
                })
            }
        }

        let mut one_packet = read_buf(packet)?;
        let mut payload = one_packet.payload.clone();
        loop{
            if payload != 0xffffff{
                break;
            }
            let tmp = read_buf(packet)?;
            payload = tmp.payload.clone();
            one_packet.larger.push(tmp);
        }


        Ok(one_packet)
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
                debug!("{}",crate::info_now_time(String::from("start parse query packet")));
                if let Err(e) = self.parse_query_packet(handler).await{
                    error!("{}",&e.to_string());
                    self.send_error_packet(handler, &e.to_string()).await?;
                    handler.per_conn_info.return_connection(0).await?;
                    //handler.per_conn_info.reset_connection().await?;
                    //return Err(Box::new(MyError(e.to_string().into())));
                };
            }
            PacketType::ComInitDb => {
                let db = readvalue::read_string_value(&self.buf[1..]);
                debug(format!("initdb {}", &db));
                if !self.check_change_db_privileges(handler, &db).await?{
                    return Ok(())
                }
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

    /// 管理命令操作，并返回数据、成功、失败等数据包
    ///
    /// 支持命令：
    ///
    /// show status: 当前服务状态， 包括各platform连接池状态，活跃连接数，qps等所有状态信息，
    /// 可添加where platform=aa只查询对应platform的状态信息
    ///
    /// show connections: 返回各节点连接信息，包括目前所有创建的连接数，活跃连接数，连接池最大最小值
    /// 可通过where platform=aa值查询对应platform的信息
    ///
    /// --platform--host_info--min_thread--max_thread--active_thread--pool_count--
    ///
    /// show questions: 返回各节点qps等状态， 同样可以添加where platform=aa
    ///
    /// --platform--host_info--com_select--com_update--com_delete--com_update--platform_questions
    ///
    /// set max_thread/min_thread=1 where platform=aa and host_info=aa: 可以修改对应节点连接池大小
    /// 但最大值必须大雨等于最小值, 如果不带任何条件则是修改所有， 如果带条件，platform为必须,
    /// 不能只有host_info, 但可以只有platform，这样是修改对应platform的所有节点
    pub async fn admin(&self, sql: &String, handler: &mut Handler, ast: &Vec<Statement>) -> Result<()> {
        let admin_sql = AdminSql::Null;
        let admin_sql = admin_sql.parse_sql(ast).await?;
        match admin_sql{
            AdminSql::Set(set_struct) => {
                if let Err(e) = handler.platform_pool.alter_pool_thread(&set_struct).await {
                    self.send_error_packet(handler, &e.to_string()).await?;
                }else {
                    self.send_ok_packet(handler).await?;
                }
            }
            AdminSql::Show(show_struct) => {
                let show_state = handler.platform_pool.show_pool_state(&show_struct).await?;
                debug!("show_state: {:?}", &show_state);
                //self.send_ok_packet(handler).await?;
                debug!("packet text response");
                let mut text_response = TextResponse::new(handler.client_flags.clone());
                if let Err(e) = text_response.packet(&show_struct, &show_state).await{
                    info!("packet text response error: {:?},", &e.to_string());
                    self.send_error_packet(handler, &e.to_string()).await?;
                }else {
                    for packet in text_response.packet_list{
                        //发送数据包
                        debug!("send text packet");
                        handler.send(&packet).await?;
                        handler.seq_add();
                    }
                    handler.reset_seq();
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// 检测platform是否为admin, 如果为admin且当前语句不是set platfrom则重置platfrom并返回false
    pub async fn check_is_admin_paltform(&self, handler: &mut Handler, sql_type: &SqlStatement) -> bool{
        if let Some(platform) = &handler.platform{
            if platform == &"admin".to_string(){
                match sql_type{
                    SqlStatement::SetVariable(k,_v) => {
                        if k.to_lowercase() == "platform".to_string(){
                            return false;
                        }
                    }
                    _ => {}
                }
                return true;
            }
        }
        return false;
    }

    async fn check_change_db_privileges(&self,  handler: &mut Handler, database: &String) -> Result<bool>{
        if let Err(e) = self.check_user_privileges(handler,  &SqlStatement::ChangeDatabase, &vec![TableInfo{ database: None, table: None }]).await{
            self.send_error_packet(handler, &e.to_string()).await?;
            return Ok(false);
        }
        return Ok(true);
    }

    async fn check_user_privileges(&self, handler: &mut Handler, sql_type: &SqlStatement, tbl_info: &Vec<TableInfo>) -> Result<()>{
        if &handler.user_name == &handler.platform_pool.config.user{
            return Ok(());
        }
        let check_privileges = CheckPrivileges::new(&handler.db, tbl_info.clone(), sql_type, &handler.user_name, &handler.host);
        check_privileges.check_user_privileges(&handler.user_privileges).await?;
        //handler.user_privileges.check_privileges(&check_privileges).await?;
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
        let dialect = MySqlDialect {};
        let sql_ast = Parser::parse_sql(&dialect, &sql)?;
        let (tbl_info_list, a, select_comment) = crate::server::sql_parser::do_table_info(&sql_ast)?;
        // let sql_parser = SqlStatement::Default;
        // let (a, _) = sql_parser.parser(&sql);
        if let Err(e) = self.check_user_privileges(handler,  &a, &tbl_info_list).await{
            self.send_error_packet(handler, &e.to_string()).await?;
            return Ok(())
        }

        if self.check_is_admin_paltform(handler, &a).await{
            self.admin(&sql, handler, &sql_ast).await?;
            return Ok(())
        }

        //info!("{}",&sql);
        debug!("{}",crate::info_now_time(String::from("parser sql sucess")));
        debug!("{}",format!("{:?}: {}", a, &sql));

        debug!("{}",crate::info_now_time(String::from("start check pool info")));

        //检查是否已经设置platform， 如果语句不为set platform语句则必须先进行platform设置，返回错误
        if !self.check_is_set_platform(&a, handler).await?{
            let error = format!("please set up a business platform first");
            error!("{}", &error);
            self.send_error_packet(handler, &error).await?;
            return Ok(())
        }

        //已经设置了platform则进行连接检查及获取
        if let Some(platform) = &handler.platform{
            if platform != &"admin".to_string(){
                handler.per_conn_info.check(&mut handler.platform_pool_on, &handler.hand_key,
                                            &handler.db, &handler.auto_commit, &a, handler.seq.clone(), select_comment, platform).await?;
                handler.per_conn_info.check_auth_save(&sql, &handler.host).await;
            }
        }

        //进行ops操作
        handler.platform_pool_on.save_com_state(&handler.per_conn_info.get_connection_host_info().await, &a).await?;

        //进行语句操作
        match a{
            SqlStatement::ChangeDatabase => {

                self.check_is_change_db(handler, &sql).await?;
                //handler.set_per_conn_cached().await?;
            }
            SqlStatement::SetVariable (variable, value) => {
                if variable.to_lowercase() == String::from("autocommit"){
                    self.set_autocommit(handler, &value).await?;
                }else if variable.to_lowercase() == String::from("platform") {
                    // 首先判断是否有未提交事务
                    if let Err(e) = handler.per_conn_info.check_have_transaction().await{
                        self.send_error_packet(handler, &e.to_string()).await?;
                        return Ok(())
                    }

                    // 设置platform
                    if handler.platform_pool.check_conn_privileges(&value, &handler.user_name).await{
                        if let Err(e) = handler.check_cur_platform(&value).await{
                            self.send_error_packet(handler, &e.to_string()).await?;
                        }else {
                            self.send_ok_packet(handler).await?;
                        }
                    }else {
                        let error = format!("current user({}) does not have permission for the platform({})", &handler.user_name, &value);
                        error!("{}", &error);
                        self.send_error_packet(handler, &error).await?;
                    }

                }else if variable.to_lowercase() == String::from("names"){
                    self.send_ok_packet(handler).await?;
                }else {
                    let error = String::from("only supports set autocommit/platform/names");
                    error!("{}", &error);
                    self.send_error_packet(handler, &error).await?;
                }
            }
            SqlStatement::Query |
            SqlStatement::Show => {
                self.exec_query(handler).await?;
            }

            SqlStatement::Commit |
            SqlStatement::Rollback |
            SqlStatement::UNLock => {
                self.send_one_packet(handler).await?;
                self.reset_is_transaction(handler).await?;
            }
            SqlStatement::Insert => {
                if self.larger.len() > 0{
                    self.send_larger_packet(handler).await?;
                }else {
                    self.send_one_packet(handler).await?;
                }
                self.check_is_no_autocommit(handler).await?;
            }
            SqlStatement::Delete |
            SqlStatement::Update => {
                self.send_one_packet(handler).await?;
                self.check_is_no_autocommit(handler).await?;
            }
            SqlStatement::StartTransaction |
            SqlStatement::Lock => {
                self.send_one_packet(handler).await?;
                self.set_is_transaction(handler).await?;
            }
            SqlStatement::AlterTable |
            SqlStatement::Create => {
                self.no_traction(handler).await?;
            }
            SqlStatement::Drop => {
                self.no_traction(handler).await?;
                self.check_drop_database(&sql, handler, &tbl_info_list).await;
            }
            SqlStatement::Comment => {
                self.send_ok_packet(handler).await?;
            }
            SqlStatement::Default => {
                let error = String::from("Unsupported syntax");
                error!("{}",&error);
                self.send_error_packet(handler, &error).await?;
            }
        }
        handler.stream_flush().await?;

        debug!("{}",crate::info_now_time(String::from("send ok")));
        Ok(())
    }

    /// 当执行drop database之后执行检查
    ///
    /// 如果删除的是当前数据库，则恢复为information_schema
    async fn check_drop_database(&self, sql: &String, handler: &mut Handler, tbl_info: &Vec<TableInfo>) {
        if sql.to_lowercase().contains("database"){
            if let Some(db) = &handler.db{
                if let Some(d) = &tbl_info[0].database{
                    if db == d {
                        handler.db = Some(String::from("information_schema"));
                    }
                }
            }
        }
    }

    /// 用于语句执行之前进行判断有没有设置platform
    async fn check_is_set_platform(&self, sql_type: &SqlStatement, handler: &mut Handler) -> Result<bool>{
        if let None = &handler.platform{
            match sql_type{
                SqlStatement::SetVariable(_variable, _value) => {
                    return Ok(true)
                }
                _ => {
                    return Ok(false);
                }
            }
        }
        return Ok(true);
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

//    async fn set_cached(&self, handler: &mut Handler) -> Result<()>{
//        let hand_key = handler.hand_key.clone();
//        if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
//            return Ok(conn_info.set_cached(&hand_key).await?);
//        }
//        return Err(Box::new(MyError(String::from("lost connection").into())));
//    }

    /// 检查是否为非自动提交， 用于数据变动时设置连接状态
    async fn check_is_no_autocommit(&self, handler: &mut Handler) -> Result<()>{
        if !handler.auto_commit{
            if let Some(conn_info) = &mut handler.per_conn_info.conn_info{
                conn_info.set_is_transaction().await?;
            }
        }
        Ok(())
    }

//    /// 检查是否为select 语句，测试用
//    async fn check_is_select(&self, handler: &mut Handler, sql: &String) -> Result<bool> {
//        if sql.to_lowercase().starts_with("select") {
//            self.exec_query(handler).await?;
//            return Ok(true)
//        }
//        return Ok(false)
//    }

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
            debug!("response:  {:?}, {:?}", &header, &buf);
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
            if header.payload < 9 && header.payload > 7{
                return (true, eof_num + 1);
            }
        }else if buf[0] == 0xfe {
            return (true, eof_num + 1);
        }
        return (false, eof_num);
    }

    /// 用于非事务性的操作
    async fn no_traction(&self, handler: &mut Handler) -> Result<()> {
        if let Some(_conn) = &mut handler.per_conn_info.conn_info{
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

    async fn send_larger_packet(&self, handler: &mut Handler) -> Result<()>{
        if let Some(conn) = &mut handler.per_conn_info.conn_info{
            let packet = self.packet_my_value();
            conn.send_packet_only(&packet).await?;

            for res in &self.larger{
                let packet = res.packet_my_value();
                conn.send_packet_only(&packet).await?;
            }
            let (buf, header) = conn.response_for_larger_packet().await?;
            self.send_mysql_response_packet(handler, &buf, &header).await?;
            //self.check_eof(handler, conn).await?;
        }
        Ok(())
    }


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
            let (buf, _header) = conn.send_packet(&packet_full).await?;
            conn.check_packet_is(&buf)?;
            return Ok(());
        }
        return Err(Box::new(MyError(String::from("lost connection").into())));
    }


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
        if !self.check_change_db_privileges(handler, &my_tmp).await?{
            return Ok(())
        }

        if let Err(e) = self.__set_default_db(my_tmp.clone(), handler).await{
            self.send_error_packet(handler, &e.to_string()).await?;
        }else {
            handler.db = Some(my_tmp);
            self.send_ok_packet(handler).await?;
        }
        return Ok(())
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
        err.extend(readvalue::write_u16(2020));
        if CLIENT_BASIC_FLAGS & CLIENT_PROTOCOL_41 > 0{
            let err_a = String::from("#HY000");
            err.extend(err_a.as_bytes());
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
