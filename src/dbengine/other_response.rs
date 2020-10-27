/*
@author: xiao cai niao
@datetime: 2020/10/26
*/

use crate::dbengine::{CLIENT_DEPRECATE_EOF, CLIENT_OPTIONAL_RESULTSET_METADATA, RESULTSET_METADATA_FULL, CLIENT_PROTOCOL_41, SERVER_STATUS_IN_TRANS, VAR_STRING};
use crate::readvalue;
use crate::mysql::query_response::{ColumnDefinition41, packet_one_column_value};
use tracing::{debug};
use crate::mysql::Result;
use crate::server::Handler;

pub enum OtherType{
    SelectUser,
    SelectMaxPacket
}
impl OtherType{
    async fn column_definitions(&self) -> Vec<Vec<u8>>{
        match self{
            OtherType::SelectUser => {
                SelectUser::packet_column_definitions().await
            }OtherType::SelectMaxPacket => {
                SelectMaxPacket::packet_column_definitions().await
            }
        }
    }
    async fn packet(&self, handler: &Handler) -> Vec<u8>{
        match self{
            OtherType::SelectUser => {
                let select_user = SelectUser::new(handler);
                select_user.packet().await
            }OtherType::SelectMaxPacket => {
                let select_max = SelectMaxPacket::new();
                select_max.packet().await
            }
        }
    }

}

/// 用于select user()
pub struct SelectUser{
    user: String
}

impl SelectUser{
    pub fn new(handler: &Handler) -> SelectUser{
        SelectUser{user: format!("{}@%", handler.user_name.clone())}
    }
    async fn packet(&self) -> Vec<u8> {
        let mut packet = vec![];
        packet.extend(packet_one_column_value(self.user.clone()).await);
        packet
    }

    async fn packet_column_definitions() -> Vec<Vec<u8>> {
        let mut packet = vec![];
        packet.push(ColumnDefinition41::show_status_column(&String::from("user()"), &VAR_STRING).await.packet().await);
        packet
    }
}


/// 用于未执行set platform之前对select @@max_allowed_packet做响应
pub struct SelectMaxPacket{
    max_allowed_packet: String,
}
impl SelectMaxPacket{
    pub fn new() -> SelectMaxPacket{
        SelectMaxPacket{max_allowed_packet: format!("{}",268435456)}
    }

    async fn packet(&self) -> Vec<u8> {
        let mut packet = vec![];
        packet.extend(packet_one_column_value(self.max_allowed_packet.clone()).await);
        packet
    }

    async fn packet_column_definitions() -> Vec<Vec<u8>> {
        let mut packet = vec![];
        packet.push(ColumnDefinition41::show_status_column(&String::from("max_allowed_packet"), &VAR_STRING).await.packet().await);
        packet
    }
}

pub struct TextResponse{
    pub packet_list: Vec<Vec<u8>>,
    pub client_flags: i32
}

impl TextResponse{
    pub fn new(client_flags: i32) -> TextResponse{
        TextResponse{ packet_list: vec![] , client_flags}
    }

    pub async fn packet(&mut self, o_type: OtherType, handler: &Handler) -> Result<()> {
        self.packet_column_count(1).await;
        self.packet_list.extend(o_type.column_definitions().await);

        debug!("{}", self.client_flags & CLIENT_DEPRECATE_EOF as i32);
        self.packet_eof().await;

        self.packet_list.push(o_type.packet(handler).await);

        // if (self.client_flags & CLIENT_DEPRECATE_EOF as i32) > 0 {
        //     debug!("ok packet");
        //     let tmp = self.ok().await;
        //     self.packet_list.push(tmp);
        // }else {
        debug!("eof packet");
        let tmp = self.eof().await;
        self.packet_list.push(tmp);
        // }

        Ok(())
    }

    async fn packet_eof(&mut self) {
        let packet = self.eof().await;
        if (self.client_flags & CLIENT_DEPRECATE_EOF as i32) <= 0{
            self.packet_list.push(packet);
        }
    }

    /// 字段个数部分以及是否跳过元数据标签
    async fn packet_column_count(&mut self, column_count: u8) {
        let mut packet = vec![];
        if (self.client_flags & CLIENT_OPTIONAL_RESULTSET_METADATA as i32) > 0{
            packet.push(RESULTSET_METADATA_FULL);
        }
        packet.push(column_count);
        self.packet_list.push(packet);
    }

    async fn eof(&mut self) -> Vec<u8> {
        let mut packet = vec![];
        packet.push(0xfe);
        if (self.client_flags & CLIENT_PROTOCOL_41 as i32) > 0{
            packet.extend(vec![0,0]);
            packet.extend(readvalue::write_u16(SERVER_STATUS_IN_TRANS as u16));
        }
        packet
    }
}
