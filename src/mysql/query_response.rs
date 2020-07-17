/*
@author: xiao cai niao
@datetime: 2020/7/17
*/

use crate::dbengine::admin::{ShowState, ShowStruct, ShowCommand, HostPoolState};
use crate::dbengine::{CLIENT_OPTIONAL_RESULTSET_METADATA, RESULTSET_METADATA_FULL};
use crate::dbengine::{CLIENT_PROTOCOL_41};
use crate::dbengine::{Long, VarString, CLIENT_DEPRECATE_EOF,SERVER_STATUS_IN_TRANS};
use crate::mysql::Result;
use crate::{MyError,readvalue};

pub struct ColumnDefinition41{
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub org_table: String,
    pub name: String,
    pub org_name: String,
    pub fix_length: u8,
    pub character_set: u16,
    pub column_length: u32,
    pub field_type: u8,
    pub flags: u16,
    pub decimals: u8
}


impl ColumnDefinition41{

    async fn show_status_column(column_name: &String, column_type: &u8) -> ColumnDefinition41{
        ColumnDefinition41{
            catalog: "def".to_string(),
            schema: "MysqlBus_system".to_string(),
            table: "MysqlBus_system_status".to_string(),
            org_table: "MysqlBus_system_status".to_string(),
            name: column_name.clone(),
            org_name: column_name.clone(),
            fix_length: 0x0c,
            character_set: crate::dbengine::UTF8MB4_UNICODE_CI as u16,
            column_length: 65535,
            field_type: column_type.clone(),
            flags: 1,
            decimals: 0
        }
    }

    async fn packet(&self) -> Vec<u8> {
        let mut packet = vec![];
        packet.extend(packet_one_column_value(self.catalog.clone()).await);
        packet.extend(packet_one_column_value(self.schema.clone()).await);
        packet.extend(packet_one_column_value(self.table.clone()).await);
        packet.extend(packet_one_column_value(self.org_table.clone()).await);
        packet.extend(packet_one_column_value(self.name.clone()).await);
        packet.extend(packet_one_column_value(self.org_name.clone()).await);
        packet.push(1);
        packet.push(self.fix_length.clone());
        packet.extend(readvalue::write_u16(self.character_set.clone()));
        packet.extend(readvalue::write_u32(self.column_length.clone()));
        packet.push(self.field_type.clone());
        packet.extend(readvalue::write_u16(self.flags.clone()));
        packet.push(self.decimals.clone());
        packet
    }

}

struct QuestionsRowValue{
    platform: String,
    host_info: String,
    com_select: String,
    com_update: String,
    com_insert: String,
    com_delete: String,
    platform_questions: String,
}
impl QuestionsRowValue{
    async fn new(platform: &String, questions: &usize, host_state: &HostPoolState) -> QuestionsRowValue{
        QuestionsRowValue{
            platform: platform.clone(),
            platform_questions: format!("{}",questions.clone()),
            com_insert: format!("{}",host_state.com_insert.clone()),
            com_delete: format!("{}",host_state.com_delete.clone()),
            com_update: format!("{}",host_state.com_update.clone()),
            com_select: format!("{}",host_state.com_select.clone()),
            host_info: host_state.host_info.clone()
        }
    }

    async fn packet(&self) -> Vec<u8> {
        let mut pakcet = vec![];
        pakcet.extend(packet_one_column_value(self.platform.clone()).await);
        pakcet.extend(packet_one_column_value(self.host_info.clone()).await);
        pakcet.extend(packet_one_column_value(self.com_select.clone()).await);
        pakcet.extend(packet_one_column_value(self.com_update.clone()).await);
        pakcet.extend(packet_one_column_value(self.com_insert.clone()).await);
        pakcet.extend(packet_one_column_value(self.com_delete.clone()).await);
        pakcet.extend(packet_one_column_value(self.platform_questions.clone()).await);
        pakcet
    }

    async fn packet_column_definitions() -> Vec<Vec<u8>> {
        let mut packet = vec![];
        packet.push(ColumnDefinition41::show_status_column(&String::from("platform"), &VarString).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("host_info"), &VarString).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("com_select"), &Long).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("com_update"), &Long).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("com_insert"), &Long).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("com_delete"), &Long).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("platform_questions"), &VarString).await.packet().await);
        packet
    }
}

struct ConnectionsRowValue{
    platform: String,
    host_info: String,
    min_thread: String,
    max_thread: String,
    active_thread: String,
    pool_count: String,
}
impl ConnectionsRowValue{
    async fn new(platform: &String, host_state: &HostPoolState) -> ConnectionsRowValue{
        ConnectionsRowValue{
            platform: platform.clone(),
            host_info: host_state.host_info.clone(),
            min_thread: format!("{}",host_state.min_thread.clone()),
            max_thread: format!("{}",host_state.max_thread.clone()),
            active_thread: format!("{}",host_state.active_thread.clone()),
            pool_count: format!("{}",host_state.active_thread.clone())
        }
    }

    async fn packet(&self) -> Vec<u8> {
        let mut pakcet = vec![];
        pakcet.extend(packet_one_column_value(self.platform.clone()).await);
        pakcet.extend(packet_one_column_value(self.host_info.clone()).await);
        pakcet.extend(packet_one_column_value(self.min_thread.clone()).await);
        pakcet.extend(packet_one_column_value(self.max_thread.clone()).await);
        pakcet.extend(packet_one_column_value(self.active_thread.clone()).await);
        pakcet.extend(packet_one_column_value(self.pool_count.clone()).await);
        pakcet
    }

    async fn packet_column_difinition() -> Vec<Vec<u8>> {
        let mut packet = vec![];
        packet.push(ColumnDefinition41::show_status_column(&String::from("platform"), &VarString).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("host_info"), &VarString).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("min_thread"), &Long).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("max_thread"), &Long).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("active_thread"), &Long).await.packet().await);
        packet.push(ColumnDefinition41::show_status_column(&String::from("pool_count"), &Long).await.packet().await);
        packet
    }
}

async fn packet_one_column_value(value: String) -> Vec<u8>{
    let mut on_value = vec![];
    on_value.push(value.len() as u8);
    on_value.extend(value.as_bytes());
    on_value
}


pub struct TextResponse{
    pub packet_list: Vec<Vec<u8>>,
    pub client_flags: i32
}

impl TextResponse{
    pub fn new(client_flags: i32) -> TextResponse{
        TextResponse{ packet_list: vec![] , client_flags}
    }

    pub async fn packet(&mut self, show_struct: &ShowStruct, show_state: &ShowState) -> Result<()> {
        match show_struct.command{
            ShowCommand::Connections => {
                self.packet_column_count(6).await;
            }
            ShowCommand::Questions => {
                self.packet_column_count(7).await;
            }
            _ => {
                return Err(Box::new(MyError(String::from("unsupported syntax").into())));
            }
        }
        self.packet_list.extend(ConnectionsRowValue::packet_column_difinition().await);
        self.packet_eof().await;
        self.packet_result_text(show_struct, show_state).await;
        if (self.client_flags & CLIENT_DEPRECATE_EOF as i32) > 0 {
            let tmp = self.ok().await;
            self.packet_list.push(tmp);
        }else {
            let tmp = self.eof().await;
            self.packet_list.push(tmp);
        }
        Ok(())
    }

    /// 数据部分
    async fn packet_result_text(&mut self, show_struct: &ShowStruct, show_state: &ShowState) {
        for pool_state in &show_state.platform_state{
            for one_pool_state in &pool_state.host_state{
                match show_struct.command{
                    ShowCommand::Questions => {
                        let c_value = QuestionsRowValue::new(&pool_state.platform, &pool_state.questions,one_pool_state).await;
                        self.packet_list.push(c_value.packet().await);
                    }
                    ShowCommand::Status => {}
                    ShowCommand::Connections => {
                        let c_value = ConnectionsRowValue::new(&pool_state.platform, &one_pool_state).await;
                        self.packet_list.push(c_value.packet().await);
                    }
                    _ => {}
                }
            }
        }
    }

    async fn packet_eof(&mut self) {
        let packet = self.eof().await;
        if (self.client_flags & CLIENT_DEPRECATE_EOF as i32) > 0{
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

    async fn ok(&mut self) -> Vec<u8> {
        let mut packet = vec![];
        packet.push(0); //packet type
        packet.push(0); //affected_rows
        packet.push(0); //last_insert_id
        if (self.client_flags & CLIENT_PROTOCOL_41 as i32) > 0{
            packet.extend(readvalue::write_u16(SERVER_STATUS_IN_TRANS as u16));
            packet.extend(vec![0,0]);    //warnings
        }
        packet
    }
}

//pub async fn test(show_state: &ShowState, show_struct: &ShowStruct, client_flags: &i32) -> Result<Vec<Vec<u8>>>{
//    let mut all_packet = vec![];
//    let mut packet = vec![];
//
//    if (client_flags & CLIENT_OPTIONAL_RESULTSET_METADATA) > 0{
//        packet.push(RESULTSET_METADATA_FULL);
//    }
//    for platform_value in show_state.platform_state{
//        match show_struct.command{
//            ShowCommand::Questions => {
//                let column_count = 6 as u8;
//                packet.push(column_count);
//                all_packet.push(packet)
//            }
//            ShowCommand::Connections => {
//                let column_count = 7 as u8;
//                packet.push(column_count);
//                all_packet.push(packet)
//            }
//            ShowCommand::Status => {
//                //以字符串形式返回
//                let column_count = 1 as u8;
//                packet.push(column_count);
//                all_packet.push(packet);
//                let show_column = ColumnDefinition41::show_status_column().await;
//            }
//            _ => {
//                let err = String::from("unsupported syntax");
//                return Err(Box::new(MyError(err.into())));
//            }
//        }
//    }
//
//    Ok(all_packet)
//}
