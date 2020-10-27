/*
@author: xiao cai niao
@datetime: 2020/5/28
*/

pub mod server;
pub mod client;
pub mod admin;
pub mod other_response;
use tracing::{debug, info};

///
/// 部分capabilities
pub const CLIENT_LONG_PASSWORD: usize = 1;
pub const CLIENT_FOUND_ROWS: usize = 2;
pub const CLIENT_LONG_FLAG: usize = 4;
pub const CLIENT_CONNECT_WITH_DB: usize = 8;
pub const CLIENT_NO_SCHEMA: usize = 16;
pub const CLIENT_COMPRESS: usize = 32;
pub const CAN_CLIENT_COMPRESS: usize = CLIENT_COMPRESS;
pub const CLIENT_ODBC: usize = 64;
pub const CLIENT_LOCAL_FILES: usize = 128;
pub const CLIENT_IGNORE_SPACE: usize = 256;
pub const CLIENT_PROTOCOL_41: usize = 512;
pub const CLIENT_INTERACTIVE: usize = 1024;
pub const CLIENT_SSL: usize = 2048;
pub const CLIENT_IGNORE_SIGPIPE: usize = 4096;
pub const CLIENT_TRANSACTIONS: usize = 8192;
pub const CLIENT_RESERVED: usize = 16384;
pub const CLIENT_RESERVED2: usize = 32768;
pub const CLIENT_MULTI_STATEMENTS: usize = 1 << 16;
pub const CLIENT_MULTI_RESULTS: usize = 1 << 17;
pub const CLIENT_PS_MULTI_RESULTS: usize = 1 << 18;
pub const CLIENT_PLUGIN_AUTH: usize = 1 << 19;
pub const CLIENT_CONNECT_ATTRS: usize = 1 << 20;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: usize = 1 << 21;
pub const CLIENT_SSL_VERIFY_SERVER_CERT: usize = 1 << 30;
pub const CLIENT_REMEMBER_OPTIONS: usize = 1 << 31;
pub const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: usize = 1 << 22;
//pub const CLIENT_SESSION_TRACK: usize = (1 << 23);
pub const CLIENT_DEPRECATE_EOF: usize = 1 << 24;
pub const CLIENT_OPTIONAL_RESULTSET_METADATA: usize = 1 << 25;
pub const CLIENT_ALL_FLAGS: usize = CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS | CLIENT_LONG_FLAG |
    CLIENT_CONNECT_WITH_DB | CLIENT_NO_SCHEMA | CLIENT_COMPRESS | CLIENT_ODBC | CLIENT_LOCAL_FILES |
    CLIENT_IGNORE_SPACE | CLIENT_PROTOCOL_41 | CLIENT_INTERACTIVE | CLIENT_SSL | CLIENT_IGNORE_SIGPIPE |
    CLIENT_TRANSACTIONS | CLIENT_RESERVED | CLIENT_RESERVED2 | CLIENT_MULTI_STATEMENTS |
    CLIENT_MULTI_RESULTS | CLIENT_PS_MULTI_RESULTS | CLIENT_SSL_VERIFY_SERVER_CERT | CLIENT_REMEMBER_OPTIONS |
    CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_ATTRS | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA |
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS | CLIENT_DEPRECATE_EOF | CLIENT_OPTIONAL_RESULTSET_METADATA;
pub const CLIENT_BASIC_FLAGS: usize =  CLIENT_ALL_FLAGS & !CLIENT_SSL & !CLIENT_COMPRESS & !CLIENT_SSL_VERIFY_SERVER_CERT;

///
/// server_status变量
pub const SERVER_STATUS_IN_TRANS: usize = 1;
pub const SERVER_STATUS_AUTOCOMMIT: usize = 2;
//pub const SERVER_MORE_RESULTS_EXISTS: usize = 8;
//pub const SERVER_QUERY_NO_GOOD_INDEX_USED: usize = 16;
//pub const SERVER_QUERY_NO_INDEX_USED: usize = 32;
//pub const SERVER_STATUS_CURSOR_EXISTS: usize = 64;
//pub const SERVER_STATUS_LAST_ROW_SENT: usize = 128;
//pub const SERVER_STATUS_DB_DROPPED: usize = 256;
//pub const SERVER_STATUS_NO_BACKSLASH_ESCAPES: usize = 512;
//pub const SERVER_STATUS_METADATA_CHANGED: usize = 1024;
//pub const SERVER_QUERY_WAS_SLOW: usize = 2048;
//pub const SERVER_PS_OUT_PARAMS: usize = 4096;
//pub const SERVER_STATUS_IN_TRANS_READONLY: usize = 8192;
//pub const SERVER_SESSION_STATE_CHANGED: usize = (1 << 14);

/// enum_resultset_metadata
///
pub const RESULTSET_METADATA_FULL: u8 = 1;  //The server will send all metadata.
//pub const RESULTSET_METADATA_NONE: u8 = 0;  //No metadata will be sent.

/// filed type
///
pub const LONG: u8 = 8;
pub const VAR_STRING: u8 = 253;

///
/// 字符集设置
pub const UTF8MB4_UNICODE_CI: usize = 224;
//pub const UTF8MB4_0900_BIN: usize = 309;
//pub const UTF8MB4_0900_AS_CI: usize = 305;
//pub const UTF8MB4_BIN: usize = 46;

pub enum PreparePacketType{
    ComStmtPrepare,
    ComStmtExecute,
    ComStmtClose,
    ComStmtReset,
    ComStmtSendLongData,
}

/// packet 类型
pub enum PacketType {
    ComQuery,
    ComQuit,
    ComPing,
    ComProcessKill,
    ComInitDb,
    ComPrepare(PreparePacketType),
    Null
}
impl PacketType{
    pub fn new(num: &u8) -> PacketType{
        info!("packet type number: {}", num);
        if num == &0x03{
            PacketType::ComQuery
        }else if num == &0x01 {
            PacketType::ComQuit
        }else if num == &0x0e {
            PacketType::ComPing
        }else if num == &0x0c {
            PacketType::ComProcessKill
        }else if num == &0x02 {
            PacketType::ComInitDb
        }else if num == &0x16 {
            PacketType::ComPrepare(PreparePacketType::ComStmtPrepare)
        }else if num == &0x17 {
            PacketType::ComPrepare(PreparePacketType::ComStmtExecute)
        }else if num == &0x19 {
            PacketType::ComPrepare(PreparePacketType::ComStmtClose)
        }else if num == &0x1a {
            PacketType::ComPrepare(PreparePacketType::ComStmtReset)
        }else if num == &0x18 {
            PacketType::ComPrepare(PreparePacketType::ComStmtSendLongData)
        }else {
            debug!("packet type number: {}", num);
            PacketType::Null
        }
    }
}

