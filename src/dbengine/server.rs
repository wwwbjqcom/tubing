/*
@author: xiao cai niao
@datetime: 2020/5/28
*/
///
/// 该模块主要功能为客户端创建socket连接时
/// 发送handshake包要求验证，并接受到客户端回包之后进行账号密码验证
///
///
use crate::dbengine::{CLIENT_BASIC_FLAGS, CLIENT_TRANSACTIONS, CAN_CLIENT_COMPRESS, CLIENT_PLUGIN_AUTH};
use crate::dbengine::{CLIENT_PROTOCOL_41, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA, CLIENT_CONNECT_WITH_DB};
use crate::dbengine::UTF8MB4_UNICODE_CI;
use crate::dbengine::{SERVER_STATUS_AUTOCOMMIT};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use crate::mysql::Result;
use byteorder::{WriteBytesExt, LittleEndian};
use crate::dbengine::client::ClientResponse;
use crate::mysql::scramble::get_sha1_pass;
use tracing::{debug, info};
use crate::readvalue;
use crate::mysql::pool::PlatformPool;
use tracing::field::debug;

#[derive(Debug, Clone)]
pub struct HandShake {
    pub code: u8,                   //协议版本好，v10
    pub server_version: String,     //服务端版本号
    pub thread_id: u32,             //分配的线程id, 随机产生的
    pub auth_plugin_data: String,   //密码加密因子, 随机产生32个字符的字符串
    pub capabilities: u32,          //状态值
    pub character_set_id: u8,       //字符集设置
    pub status_flags: u16,
    pub auth_plugin_name: String
}
impl HandShake {
    ///
    /// 初始化handshake数据
    ///
    pub fn new() -> HandShake {
        let capabilities = CLIENT_BASIC_FLAGS | CLIENT_TRANSACTIONS | CAN_CLIENT_COMPRESS;
        let auth_plugin_data: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(21)
            .collect();
        HandShake {
            code: 10,
            server_version: String::from(crate::VERSION),
            thread_id: rand::thread_rng().gen::<u32>(),
            auth_plugin_data,
            capabilities: capabilities as u32,
            character_set_id: UTF8MB4_UNICODE_CI as u8,
            status_flags: SERVER_STATUS_AUTOCOMMIT as u16,
            auth_plugin_name: String::from("mysql_native_password")
        }
    }

    ///
    /// 封装server发送的handshake包
    ///
    pub fn server_handshake_packet(&self) -> Result<Vec<u8>>{
        let mut buf: Vec<u8> = vec![];
        buf.push(self.code);
        buf.extend(self.server_version.as_bytes());
        buf.push(0);
        buf.extend(self.write_u32(&self.thread_id)?);
        let auth_plugin_data_bytes = self.auth_plugin_data.as_bytes();
        let auth_plugin_data_len = auth_plugin_data_bytes.len();
        buf.extend(&auth_plugin_data_bytes[0..8]);
        buf.push(0);
        buf.extend(&self.capabilities.to_le_bytes()[2..]);
        buf.push(self.character_set_id);
        buf.extend(self.write_u16(&self.status_flags)?);
        buf.extend(&self.capabilities.to_le_bytes()[0..2]);
        buf.push(auth_plugin_data_len as u8);
        buf.extend(&[0 as u8; 10]);
        buf.extend(&auth_plugin_data_bytes[8..]);
        buf.extend(self.auth_plugin_name.as_bytes());
        buf.push(0);
        Ok(buf)
    }

    fn write_u32(&self, num: &u32) -> Result<Vec<u8>> {
        let mut rdr = Vec::new();
        rdr.write_u32::<LittleEndian>(num.clone())?;
        return Ok(rdr);
    }

    fn write_u16(&self, num: &u16) -> Result<Vec<u8>> {
        let mut rdr = Vec::new();
        rdr.write_u16::<LittleEndian>(num.clone())?;
        return Ok(rdr);
    }

    pub async fn auth(&self, response: &ClientResponse,
                      status_flags: u16, platform_pool: &PlatformPool) -> Result<(Vec<u8>, Option<String>, i32, String, bool)>{
        let client_flags = readvalue::read_i32(&response.buf);

        let mut offset = 32;
        let mut db = None;
        //获取user_name
        let mut index= 0 ;
        for (b,item )in response.buf[offset..].iter().enumerate() {
            if item == &0x00 {
                index = b;
                break;
            }
        }
        let user_name = readvalue::read_string_value(&response.buf[offset..offset + index]);
        offset += index + 1;
        //匹配用户名
        let user_info_lock = platform_pool.user_info.read().await;
        if !user_info_lock.check_user_name(&user_name){
            //handler.send(&self.error_packet(String::from("wrong username")).await?).await?;
            return Ok((self.error_packet(String::from("wrong username")).await?, db, client_flags, user_name, false));
        }
        drop(user_info_lock);

        let mut password = vec![];
        let mut auth_name = "".to_string();
        //获取密码
        if self.capabilities & (CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA as u32) > 0 {
            let password_len = response.buf[offset..offset+1][0];
            password = response.buf[offset+1..offset+1+ password_len as usize].to_vec();
            debug!("offset:{}, password_len:{}, {:?}",&offset, &password_len, &password);
            offset += (1 + password_len) as usize;

            // if &password != &auth_password{
            //     //handler.send(&self.error_packet(String::from("wrong password")).await?).await?;
            //     return Ok((self.error_packet(String::from("wrong password")).await?, db, client_flags, user_name));
            // }
        }

        if client_flags & CLIENT_CONNECT_WITH_DB as i32 > 0{
            let mut index= 0 ;
            for (b,item )in response.buf[offset..].iter().enumerate() {
                if item == &0x00 {
                    index = b;
                    break;
                }
            }
            let tmp_db = readvalue::read_string_value(&response.buf[offset..offset + index]);
            if tmp_db != String::from(""){
                db = Some(tmp_db)
            }
            debug!("db: {:?}, offset:{}", &db, &offset);
        }

        if client_flags & CLIENT_PLUGIN_AUTH as i32 > 0 {
            let mut index= 0 ;
            for (b,item )in response.buf[offset..].iter().enumerate() {
                if item == &0x00 {
                    index = b;
                    break;
                }
            }
            auth_name = readvalue::read_string_value(&response.buf[offset..offset + index]);
            debug!("auth_name: {}, offset:{}", &auth_name, &offset);
        }
        let my_auth_password = self.get_password_auth(&auth_name, &user_name, platform_pool).await?;
        debug!("client: {:?}", &password);
        debug!("server: {:?}", &my_auth_password);
        if password.len() == 0{
            //switch request
            return Ok((self.switch_request().await, db, client_flags, user_name, true));
        }

        if &password != &my_auth_password{
            //handler.send(&self.error_packet(String::from("wrong password")).await?).await?;
            return Ok((self.error_packet(String::from("wrong password")).await?, db, client_flags, user_name, false));
        }

        return Ok((self.ok_packet(status_flags).await?, db, client_flags, user_name, false));
    }

    pub async fn switch_auth(&self, response: &ClientResponse, platform_pool: &PlatformPool, user_name: &String, status_flags: u16) -> Result<Vec<u8>> {
        let my_auth_password = self.get_password_auth(&self.auth_plugin_name, &user_name, platform_pool).await?;
        let password = response.buf.to_vec();
        if my_auth_password != password{
            return Ok(self.error_packet(String::from("wrong password")).await?);
        }
        return Ok(self.ok_packet(status_flags).await?);
    }

    async fn switch_request(&self) -> Vec<u8> {
        let mut switch: Vec<u8> = vec![];
        switch.push(0xfe);
        switch.extend(self.auth_plugin_name.as_bytes());
        switch.push(0);
        switch.extend(self.auth_plugin_data.as_bytes());
        switch
    }

    async fn get_password_auth(&self, auth_name: &String, user_name: &String, platform_pool: &PlatformPool) -> Result<Vec<u8>> {
        debug!("{}", auth_name);
        let user_info_lock = platform_pool.user_info.read().await;
        let auth_password = user_info_lock.get_user_password(user_name);
        let new_auth_password;
        debug!("{}", &auth_password);
        if auth_name != &"".to_string(){
            new_auth_password = get_sha1_pass(&auth_password, auth_name, &self.auth_plugin_data.clone().into_bytes());
        }else {
            new_auth_password = get_sha1_pass(&auth_password, &self.auth_plugin_name, &self.auth_plugin_data.clone().into_bytes());
        }

        debug!("{:?}",&new_auth_password);
        Ok(new_auth_password)

    }

    async fn error_packet(&self, error: String) -> Result<Vec<u8>>{
        let mut err = vec![];
        err.push(0xff);
        err.extend(readvalue::write_u16(10));
        if self.status_flags & CLIENT_PROTOCOL_41 as u16 > 0 {
            for _ in 0..5{
                err.push(0);
            }
        }
        err.extend(error.as_bytes());
        Ok(err)
    }


    async fn ok_packet(&self, status_flags: u16) -> Result<Vec<u8>>{
        let mut packet = vec![];
        packet.push(0); //packet type
        packet.push(0); //affected_rows
        packet.push(0); //last_insert_id
        if CLIENT_BASIC_FLAGS & CLIENT_PROTOCOL_41 > 0{
            packet.extend(readvalue::write_u16(status_flags));
            packet.extend(vec![0,0]);    //warnings
        }
        Ok(packet)
    }
}