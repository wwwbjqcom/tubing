/*
@author: xiao cai niao
@datetime: 2020/7/24
*/
use crate::mysql::Result;
use crate::{readvalue, MyError};
use crate::mysql::connection::AllUserInfo;
use crate::server::sql_parser::SqlStatement;
use crate::mysql::pool::{PlatformPool, MysqlConnectionInfo};
use std::collections::HashMap;
use tracing::{debug, info};
use tokio::sync::RwLock;
use std::sync::Arc;
use serde::de::value::StringDeserializer;

trait CheckSqlType{
    fn check_sql_type(&self, sql_type: &SqlStatement) -> bool;
    fn check_show_type(&self) -> bool;
}

impl CheckSqlType for User{
    fn check_sql_type(&self, sql_type: &SqlStatement) -> bool{
        return match sql_type {
            SqlStatement::Query => self.select.clone(),
            SqlStatement::Delete => self.delete.clone(),
            SqlStatement::Insert => self.insert.clone(),
            SqlStatement::Update => self.update.clone(),
            SqlStatement::Create => self.create.clone(),
            SqlStatement::Drop => self.drop.clone(),
            SqlStatement::AlterTable => self.alter.clone(),
            SqlStatement::ChangeDatabase => self.check_show_type(),
            SqlStatement::Show => self.select.clone(),
            _ => {
                false
            }
        }
    }
    fn check_show_type(&self) -> bool{
        if self.select || self.delete || self.insert || self.update || self.create || self.drop || self.alter{
            return true;
        }
        return false;
    }

}

impl CheckSqlType for DBPri{
    fn check_sql_type(&self, sql_type: &SqlStatement) -> bool{
        return match sql_type {
            SqlStatement::Query => self.select.clone(),
            SqlStatement::Show => self.select.clone(),
            SqlStatement::Delete => self.delete.clone(),
            SqlStatement::Insert => self.insert.clone(),
            SqlStatement::Update => self.update.clone(),
            SqlStatement::Create => self.create.clone(),
            SqlStatement::Drop => self.drop.clone(),
            SqlStatement::AlterTable => self.alter.clone(),
            SqlStatement::ChangeDatabase => self.check_show_type(),
            _ => {
                false
            }
        }
    }
    fn check_show_type(&self) -> bool{
        if self.select || self.delete || self.insert || self.update || self.create || self.drop || self.alter{
            return true;
        }
        return false;
    }
}

impl CheckSqlType for TablePri{
    fn check_sql_type(&self, sql_type: &SqlStatement) -> bool{
        return match sql_type {
            SqlStatement::Query => self.select.clone(),
            SqlStatement::Show => self.select.clone(),
            SqlStatement::Delete => self.delete.clone(),
            SqlStatement::Insert => self.insert.clone(),
            SqlStatement::Update => self.update.clone(),
            SqlStatement::Create => self.create.clone(),
            SqlStatement::Drop => self.drop.clone(),
            SqlStatement::AlterTable => self.alter.clone(),
            SqlStatement::ChangeDatabase => self.check_show_type(),
            _ => {
                false
            }
        }
    }
    fn check_show_type(&self) -> bool{
        if self.select || self.delete || self.insert || self.update || self.create || self.drop || self.alter{
            return true;
        }
        return false;
    }
}

//查询数据时mysql返回的字段元数据
#[derive(Debug, Clone)]
pub struct MetaColumn{
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub org_table: String,
    pub name: String,
    pub org_name: String,
    pub character_set: u16,
    pub column_length: u32,
    pub column_type: u8,
    pub flag: u16
}

impl MetaColumn{
    pub fn new(buf: &Vec<u8>) -> MetaColumn {
        let mut offset: usize = 0;
        let mut var_size = buf[0] as usize ; //字段信息所占长度
        offset += 1;
        let catalog = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let schema = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let table = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let org_table = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let name = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        var_size = buf[offset] as usize;
        offset += 1;
        let org_name = readvalue::read_string_value(&buf[offset..offset+var_size]);
        offset += var_size;

        offset += 1;

        let character_set = readvalue::read_u16(&buf[offset..offset+2]);
        offset += 2;

        let column_length = readvalue::read_u32(&buf[offset..offset+4]);
        offset += 4;

        let column_type = buf[offset];
        offset +=1;

        let flag = readvalue::read_u16(&buf[offset..offset+2]);

        MetaColumn{
            catalog,
            schema,
            table,
            org_table,
            name,
            org_name,
            character_set,
            column_length,
            column_type,
            flag
        }
    }
}

/// 对应mysql中user表信息
#[derive(Clone, Debug)]
pub struct User{
    pub host: String,
    pub user: String,
    pub select: bool,
    pub update: bool,
    pub insert: bool,
    pub delete: bool,
    pub create: bool,
    pub drop: bool,
    pub alter: bool
}
impl User{
    fn new() -> User{
        User{
            host: "".to_string(),
            user: "".to_string(),
            select: false,
            update: false,
            insert: false,
            delete: false,
            create: false,
            drop: false,
            alter: false
        }
    }

    fn init_value(&mut self, row_value: &HashMap<String, String>) {
        if let Some(v) = row_value.get(&"Host".to_string()){
            self.host = v.clone();
        }
        if let Some(v) = row_value.get(&"User".to_string()){
            self.user = v.clone();
        }
        if let Some(v) = row_value.get(&"Select_priv".to_string()){
            if v == &"Y".to_string(){
                self.select = true;
            }
        }
        if let Some(v) = row_value.get(&"Insert_priv".to_string()){
            if v == &"Y".to_string(){
                self.insert = true;
            }
        }
        if let Some(v) = row_value.get(&"Update_priv".to_string()){
            if v == &"Y".to_string(){
                self.update = true;
            }
        }
        if let Some(v) = row_value.get(&"Delete_priv".to_string()){
            if v == &"Y".to_string(){
                self.delete = true;
            }
        }
        if let Some(v) = row_value.get(&"Create_priv".to_string()){
            if v == &"Y".to_string(){
                self.create = true;
            }
        }
        if let Some(v) = row_value.get(&"Drop_priv".to_string()){
            if v == &"Y".to_string(){
                self.drop = true;
            }
        }
        if let Some(v) = row_value.get(&"Alter_priv".to_string()){
            if v == &"Y".to_string(){
                self.alter = true;
            }
        }
    }
}

/// 对应mysql中table_pri表
#[derive(Clone, Debug)]
pub struct TablePri{
    pub host: String,
    pub user: String,
    pub db: String,
    pub table: String,
    pub select: bool,
    pub update: bool,
    pub insert: bool,
    pub delete: bool,
    pub create: bool,
    pub drop: bool,
    pub alter: bool
}

impl TablePri{
    fn new() -> TablePri{
        TablePri{
            host: "".to_string(),
            user: "".to_string(),
            db: "".to_string(),
            table: "".to_string(),
            select: false,
            update: false,
            insert: false,
            delete: false,
            create: false,
            drop: false,
            alter: false
        }
    }

    fn init_value(&mut self, row_value: &HashMap<String, String>){
        if let Some(v) = row_value.get(&"Host".to_string()){
            self.host = v.clone();
        }
        if let Some(v) = row_value.get(&"User".to_string()){
            self.user = v.clone();
        }
        if let Some(v) = row_value.get(&"Db".to_string()){
            self.db = v.clone();
        }
        if let Some(v) = row_value.get(&"Table_name".to_string()){
            self.table = v.clone();
        }
        if let Some(v) = row_value.get(&"Table_priv".to_string()){
            let value = v.to_lowercase();
            let value_ver = value.split(",");
            let value_ver = value_ver.collect::<Vec<&str>>();
            for i in &value_ver{
                match i.as_ref(){
                    "select" => self.select = true,
                    "update" => self.update = true,
                    "delete" => self.delete = true,
                    "insert" => self.insert = true,
                    "alter" => self.alter = true,
                    "drop" => self.drop = true,
                    "create" => self.create = true,
                    _ => {}
                }
            }
        }
    }
}

/// 对应mysql中db表
#[derive(Clone, Debug)]
pub struct DBPri{
    pub host: String,
    pub user: String,
    pub db: String,
    pub select: bool,
    pub update: bool,
    pub insert: bool,
    pub delete: bool,
    pub create: bool,
    pub drop: bool,
    pub alter: bool
}

impl DBPri{
    fn new() -> DBPri{
         DBPri{
            host: "".to_string(),
            user: "".to_string(),
            db: "".to_string(),
            select: false,
            update: false,
            insert: false,
            delete: false,
            create: false,
            drop: false,
            alter: false
        }

    }

    fn init_value(&mut self, row_value: &HashMap<String, String>){
        if let Some(v) = row_value.get(&"Host".to_string()){
            self.host = v.clone();
        }

        if let Some(v) = row_value.get(&"User".to_string()){
            self.user = v.clone();
        }
        if let Some(v) = row_value.get(&"Db".to_string()){
            self.db = v.clone();
        }
        if let Some(v) = row_value.get(&"Select_priv".to_string()){
            if v == &"Y".to_string(){
                self.select = true;
            }
        }
        if let Some(v) = row_value.get(&"Update_priv".to_string()){
            if v == &"Y".to_string(){
                self.update = true;
            }
        }
        if let Some(v) = row_value.get(&"Delete_priv".to_string()){
            if v == &"Y".to_string(){
                self.delete = true;
            }
        }
        if let Some(v) = row_value.get(&"Insert_priv".to_string()){
            if v == &"Y".to_string(){
                self.insert = true;
            }
        }
        if let Some(v) = row_value.get(&"Create_priv".to_string()){
            if v == &"Y".to_string(){
                self.create = true;
            }
        }
        if let Some(v) = row_value.get(&"Drop_priv".to_string()){
            if v == &"Y".to_string(){
                self.drop = true;
            }
        }
        if let Some(v) = row_value.get(&"Alter_priv".to_string()){
            if v == &"Y".to_string(){
                self.alter = true;
            }
        }

    }
}


/// 记录用户权限信息
#[derive(Clone, Debug)]
pub struct UserPri{
    pub user: String,                   //用户名
    pub user_pri: Option<Vec<User>>,
    pub db_pri: Option<Vec<DBPri>>,
    pub table_pri: Option<Vec<TablePri>>,
    pub platform: String
}

impl UserPri{
    pub fn new(user: &String, platform: &String) -> UserPri{
        UserPri{
            user: user.clone(),
            user_pri: None,
            db_pri: None,
            table_pri: None,
            platform: platform.clone()
        }
    }

    /// 检查用户是否存在， 多个db可能有相同的用户名，所以这里同时匹配platform
    ///
    /// 当platform位none时代表还没有设置platform， 如果存在用户则直接返回成功
    pub fn check_user_name(&self, user_name: &String, platform: &Option<String>) -> bool{
        if user_name == &self.user{
            return if let Some(pl) = platform {
                if pl == &self.platform {
                    true
                } else {
                    false
                }
            } else {
                true
            }
        }
        return false
    }

    async fn check_privileges(&self, check_struct: &CheckPrivileges, tbl_info: &TableInfo) -> Result<()>{
        if self.check_user_privileges(check_struct).await{return Ok(())}
        if self.check_db_privileges(check_struct,tbl_info).await{return Ok(())}
        if self.check_table_privileges(check_struct,tbl_info).await{return Ok(())}
        return Err(Box::new(MyError(self.get_error_string(tbl_info,check_struct).into())));
    }

    fn get_error_string(&self, tbl_info: &TableInfo, check_struct: &CheckPrivileges) -> String{
        let err;
        if let Some(db) = &tbl_info.database{
            match check_struct.sql_type {
                SqlStatement::ChangeDatabase => {
                    err = format!("Access denied for user '{}'@'{}' to database '{}'", &check_struct.user_name, &check_struct.host, db);
                }
                _ => {
                    err = format!("Access denied for user '{}'@'{}' to table '{}.{}'", &check_struct.user_name, &check_struct.host, db, &tbl_info.get_table_name());
                }
            }
        }else {
            err = format!("Access denied for user '{}'@'{}' to table '{}.{}'", &check_struct.user_name, &check_struct.host, &check_struct.cur_db, &tbl_info.get_table_name());
        }
        return err;
    }

    async fn check_user_privileges(&self, check_struct: &CheckPrivileges) -> bool{
        if let Some(user_pri_all) = &self.user_pri{
            for user_pri in user_pri_all{
                if check_host(&check_struct.host, &user_pri.host){
                    return user_pri.check_sql_type(&check_struct.sql_type);
                    //return self.check_sql_privileges(db_pri, check_struct).await;
                }
            }
        }
        return false;
    }

    async fn check_db_privileges(&self, check_struct: &CheckPrivileges, tbl_info: &TableInfo) -> bool{
        if let Some(db_pri_all) = &self.db_pri{
            for db_pri in db_pri_all{
                if let Some(tbl_db) = &tbl_info.database {
                    if tbl_db == &db_pri.db && check_host(&check_struct.host, &db_pri.host){
                        return db_pri.check_sql_type(&check_struct.sql_type);
                    }
                }else {
                    if &check_struct.cur_db == &db_pri.db && check_host(&check_struct.host, &db_pri.host){
                        return db_pri.check_sql_type(&check_struct.sql_type);
                    }
                }
            }
        }
        return false;
    }

    async fn check_table_privileges(&self, check_struct: &CheckPrivileges, tbl_info: &TableInfo) -> bool{
        if let Some(tbl_pri_all) = &self.table_pri{
            for tbl_pri in tbl_pri_all{
                if check_host(&check_struct.host, &tbl_pri.host) && check_struct.check_cur_sql_table_info(&tbl_pri.db, &tbl_pri.table, tbl_info){
                    return tbl_pri.check_sql_type(&check_struct.sql_type);
                }
            }
        }
        return false;
    }

    // async fn check_sql_privileges<F>(&self, pri_info: &F, check_struct: &CheckPrivileges) -> bool{
    //     return match check_struct.sql_type {
    //         SqlStatement::Query => pri_info.select,
    //         SqlStatement::Delete => pri_info.delete,
    //         SqlStatement::Insert => pri_info.insert,
    //         SqlStatement::Update => pri_info.update,
    //         SqlStatement::Create => pri_info.create,
    //         SqlStatement::Drop => pri_info.drop,
    //         SqlStatement::AlterTable => pri_info.alter,
    //         _ => {
    //             false
    //         }
    //     }
    // }
}


/// 保存对应db中已有的所有用户名
///
/// 用于加载权限信息
#[derive(Debug, Clone)]
pub struct PlatformUserInfo{
    pub user_name: String,  //用户名
    pub platform: String,   //所属platform
}


impl PlatformUserInfo{
    fn init_value(row_value: &HashMap<String, String>, platform: String) -> Option<PlatformUserInfo> {
        return if let Some(v) = row_value.get(&"user".to_string()) {
            Some(PlatformUserInfo { user_name: v.clone(), platform })
        } else {
            None
        }
    }
}


/// 保存所有platform db中用户权限的结构体
///
#[derive(Clone, Debug)]
pub struct AllUserPri{
    pub all_user_info: Arc<RwLock<Vec<PlatformUserInfo>>>,    //所有用户
    pub all_pri: Arc<RwLock<Vec<UserPri>>>,                      // 用户权限
    pub platform_pool: PlatformPool                 //所有platfrom的连接池
}

impl AllUserPri{
    pub fn new(platform_pool: &PlatformPool) -> AllUserPri{
        AllUserPri{
            all_user_info: Arc::new(RwLock::new(vec![])),
            all_pri: Arc::new(RwLock::new(vec![])),
            platform_pool: platform_pool.clone()
        }
    }

    /// 从all_user_info中获取所有用户列表
    ///
    /// 由于rust所有权问题，在一个函数中获取锁读取数据，再利用该数据进行其他结构体数据操作会发生错误
    ///
    /// 所有这里单独一个函数获取一个临时列表
    async fn get_user_info_list(&mut self) -> Vec<PlatformUserInfo>{
        let read_user_info_lock = self.all_user_info.read().await;
        let mut tmp_user_info_list = vec![];
        for user_info in &*read_user_info_lock{
            tmp_user_info_list.push(user_info.clone())
        }
        return tmp_user_info_list
    }

    /// 获取用户权限列表， 用于对单独platform进行权限重载
    async fn get_user_pri_list(&mut self) -> Vec<UserPri> {
        let read_user_pri_lock = self.all_pri.read().await;
        let mut tmp_user_pri_list = vec![];
        for user_pri in &*read_user_pri_lock{
            tmp_user_pri_list.push(user_pri.clone())
        }
        return tmp_user_pri_list
    }

    /// 获取所有用户权限列表
    ///
    /// 用于全局重载用户权限和初始化的时候
    pub async fn get_pris(&mut self) -> Result<()>{
        debug!("get all user privileges");
        // 获取platfrom列表， 并通过platfrom从连接池获取相应链接，在查询出所有用户用户信息
        let platform_list = self.platform_pool.get_platform_list().await;
        self.reload_all_user_info(platform_list).await?;
        //
        let tmp_user_info_list = self.get_user_info_list().await;
        // 存储所有权限的临时列表
        let mut tmp_all_pri = vec![];
        for user_info in &tmp_user_info_list {
            let mut one_user_pri = UserPri::new(&user_info.user_name, &user_info.platform);
            if let (Some(mut platform_pool_on), _) = self.platform_pool.get_platform_pool(&user_info.platform).await{
                let (mut mysql_conn, mut mysql_conn_pool) =platform_pool_on.get_pool(&SqlStatement::Query,
                                                                   &"".to_string(), &None,
                                                                   &user_info.platform, false).await?;
                self.get_user_pri(&mut mysql_conn, &mut one_user_pri).await?;
                self.get_db_pri(&mut mysql_conn, &mut one_user_pri).await?;
                self.get_table_pri(&mut mysql_conn, &mut one_user_pri).await?;
                mysql_conn_pool.return_pool(mysql_conn, &user_info.platform).await?;
                tmp_all_pri.push(one_user_pri);
            }
        }
        // 获取全局写锁， 清空当前权限数据，并附加临时权限列表进去
        let mut all_pri_write_lock = self.all_pri.write().await;
        all_pri_write_lock.clear();
        all_pri_write_lock.extend(tmp_all_pri);
        debug!("all user privileges: {:?}", &self.all_pri);
        Ok(())
    }

    /// 获取user表中权限信息
    async fn get_user_pri(&mut self, mysql_conn: &mut MysqlConnectionInfo, user_pri: &mut UserPri) -> Result<()>{
        let sql = format!("select * from mysql.user where user='{}'",user_pri.user);
        let value = mysql_conn.execute_command(&sql).await?;
        let mut pri = vec![];
        for row_value in value{
            let mut new_user_pri = User::new();
            new_user_pri.init_value(&row_value);
            pri.push(new_user_pri);
        }
        if pri.len() > 0{
            user_pri.user_pri = Some(pri);
        }
        Ok(())
    }

    /// 获取db表中权限信息
    async fn get_db_pri(&mut self, mysql_conn: &mut MysqlConnectionInfo, user_pri: &mut UserPri) -> Result<()>{
        let sql = format!("select * from mysql.db where user='{}'",user_pri.user);
        let value = mysql_conn.execute_command(&sql).await?;
        let mut db_pri = vec![];
        for row_value in value{
            let mut new_db_pri = DBPri::new();
            new_db_pri.init_value(&row_value);
            db_pri.push(new_db_pri);
        }
        if db_pri.len() > 0{
            user_pri.db_pri = Some(db_pri);
        }
        Ok(())
    }

    /// 获取table_pri表中的table权限信息
    async fn get_table_pri(&mut self, mysql_conn: &mut MysqlConnectionInfo, user_pri: &mut UserPri) -> Result<()>{
        let sql = format!("select * from mysql.tables_priv where user='{}'",user_pri.user);
        let value = mysql_conn.execute_command(&sql).await?;
        let mut tbl_pri = vec![];
        for row_value in value{
            let mut new_tbl_pri = TablePri::new();
            new_tbl_pri.init_value(&row_value);
            tbl_pri.push(new_tbl_pri);
        }
        if tbl_pri.len() > 0{
            user_pri.table_pri = Some(tbl_pri);
        }
        Ok(())
    }




    /// 重载用户信息的操作
    ///
    /// 如果有platform代表只重载该业务集群的用户信息
    ///
    /// 如果没有则全部重载， 就需把所有用户、用户权限重制
    pub async fn reload_user_pri(&mut self, platform: Option<String>) -> Result<()> {
        return if let Some(pl) = platform{
            let new_platform_user_info_list = self.reload_platform_user_info(pl.clone()).await?;
            self.reload_platform_user_pri(pl.clone(), &new_platform_user_info_list).await?;
            Ok(())
        }else {
            self.reset_user_info().await;
            Ok(self.get_pris().await?)
        }
    }

    /// 重载某个platform的用户权限信息
    ///
    /// platform_user_info_list:  通过reload_platform_user_info函数返回最新用户信息列表
    async fn reload_platform_user_pri(&mut self, platform: String, platform_user_info_list: &Vec<PlatformUserInfo>) -> Result<()> {
        // 获取当前所有用户权限信息
        // 剔除需要重载的platform用户信息
        let user_pri_list = self.get_user_pri_list().await;
        let mut tmp_user_pri_list = vec![];
        for user_pri in user_pri_list{
            if &user_pri.platform != &platform {
                tmp_user_pri_list.push(user_pri);
            }
        }

        // 从连接池中获取对应platform的链接， 并利用链接进行权限信息获取
        for user_info in platform_user_info_list {
            let mut one_user_pri = UserPri::new(&user_info.user_name, &user_info.platform);
            if let (Some(mut platform_pool_on), _) = self.platform_pool.get_platform_pool(&user_info.platform).await{
                let (mut mysql_conn, mut mysql_conn_pool) =platform_pool_on.get_pool(&SqlStatement::Query,
                                                                                     &"".to_string(), &None,
                                                                                     &user_info.platform, false).await?;
                self.get_user_pri(&mut mysql_conn, &mut one_user_pri).await?;
                self.get_db_pri(&mut mysql_conn, &mut one_user_pri).await?;
                self.get_table_pri(&mut mysql_conn, &mut one_user_pri).await?;
                mysql_conn_pool.return_pool(mysql_conn, &user_info.platform).await?;
                tmp_user_pri_list.push(one_user_pri);
            }
        }

        //获取权限信息写锁， 并清空添加最新信息回去
        let mut write_all_user_pri = self.all_pri.write().await;
        write_all_user_pri.clear();
        write_all_user_pri.extend(tmp_user_pri_list);
        Ok(())
    }

    /// 重载某一个platform的用户信息, 并返回新的用户信息列表
    async fn reload_platform_user_info(&mut self, platform: String) -> Result<Vec<PlatformUserInfo>> {
        // 获取当前所有platform的用户信息
        // 从当前用户信息中删除要重载的platform信息
        let user_info_list = self.get_user_info_list().await;
        let mut tmp_user_info_list = vec![];
        let mut tmp_new_user_info_list = vec![];
        for user_info in user_info_list{
            if &user_info.platform != &platform {
                tmp_user_info_list.push(user_info);
            }
        }

        // 从连接池中获取对应platform的链接， 并获取到所有用户信息
        if let (Some(mut platform_pool_on), _) = self.platform_pool.get_platform_pool(&platform).await {
            let (mut mysql_conn, mut mysql_conn_pool) = platform_pool_on.get_pool(&SqlStatement::Query,
                                                                                  &"".to_string(), &None,
                                                                                  &platform, false).await?;
            let one_platform_user_info = self.get_db_all_user_info(&mut mysql_conn, &platform).await?;
            mysql_conn_pool.return_pool(mysql_conn, &platform).await?;
            tmp_new_user_info_list.extend(one_platform_user_info.clone());
            tmp_user_info_list.extend(one_platform_user_info);
        }

        // 获取写锁， 清空当前信息， 并把所有用户信息重新写入
        let mut write_all_info_lock = self.all_user_info.write().await;
        write_all_info_lock.clear();
        write_all_info_lock.extend(tmp_user_info_list);
        Ok(tmp_new_user_info_list)
    }

    /// 重制用户信息
    async fn reset_user_info(&mut self) {
        // let mut pri_info = self.all_pri.write().await;
        // pri_info.clear();
        // drop(pri_info);
        let mut all_user_info = self.all_user_info.write().await;
        all_user_info.clear();
    }


    /// 获取所有用户信息
    async fn reload_all_user_info(&mut self, platform_list: Vec<String>) -> Result<()> {
        for platform in platform_list {
            if let (Some(mut platform_pool_on), _) = self.platform_pool.get_platform_pool(&platform).await {
                let (mut mysql_conn, mut mysql_conn_pool) = platform_pool_on.get_pool(&SqlStatement::Query,
                                                                                      &"".to_string(), &None,
                                                                                      &platform, false).await?;
                let one_platform_user_info = self.get_db_all_user_info(&mut mysql_conn, &platform).await?;
                mysql_conn_pool.return_pool(mysql_conn, &platform).await?;

                // 获取写锁， 并把一个platform的用户信息写入列表
                let mut write_all_info_lock = self.all_user_info.write().await;
                write_all_info_lock.extend(one_platform_user_info);
                drop(write_all_info_lock);
            }
        }
        Ok(())
    }

    /// 获取每个paltform对应所有用户信息
    async fn get_db_all_user_info(&mut self,  mysql_conn: &mut MysqlConnectionInfo, platform: &String) -> Result<Vec<PlatformUserInfo>> {
        let sql = String::from("select user from mysql.user group by user");
        let value = mysql_conn.execute_command(&sql).await?;
        let mut tmp_user_info_list = vec![];
        for row_value in value{
            if let Some(user_info) = PlatformUserInfo::init_value(&row_value, platform.clone()){
                tmp_user_info_list.push(user_info);
            }
        }
        Ok(tmp_user_info_list)
    }

    // /// 判断用户权限
    // ///
    // /// 按权限从大到小的优先级进行判断，新判断user表中、再db表最后判断table表
    // pub async fn check_privileges(&self, check_struct: &CheckPrivileges) -> Result<()>{
    //     for user_pri in &self.all_pri{
    //         if user_pri.check_user_name(&check_struct.user_name){
    //             user_pri.check_privileges(check_struct).await?;
    //             return Ok(())
    //         }
    //     }
    //     let err = format!("Access denied for user '{}'@'{}'", &check_struct.user_name, &check_struct.host);
    //     return Err(Box::new(MyError(err.into())))
    // }

}

#[derive(Clone, Debug)]
pub struct TableInfo{
    pub database: Option<String>,
    pub table: Option<String>
}

impl TableInfo{
    // pub fn get_database_name(&self) -> String{
    //     if let Some(db) = &self.database{
    //         return db.clone();
    //     }
    //     return "".to_string();
    // }

    pub fn get_table_name(&self) -> String{
        if let Some(tb) = &self.table{
            return tb.clone();
        }
        return "".to_string();
    }
}

/// 检查权限所需的结构体
#[derive(Clone, Debug)]
pub struct CheckPrivileges{
    pub cur_db: String,                         //记录当前连接默认db
    pub cur_sql_table_info: Vec<TableInfo>,     //记录当前执行sql使用的db、table信息
    pub sql_type: SqlStatement,                 //当前sql类型
    pub user_name: String,                      //当前使用的用户名
    pub host: String,                           //当前连接愿IP
    pub platform: Option<String>                //当前使用的platform
}
impl CheckPrivileges{
    pub fn new(cur_db: &Option<String>, sql_table_info: Vec<TableInfo>, sql_type: &SqlStatement, user_name: &String, host: &String, platform: &Option<String>) -> CheckPrivileges{
        let mut my_cur_db;
        if let Some(db) = cur_db{
            my_cur_db = db.clone();
        }else {
            my_cur_db = "".to_string();
        }

        match sql_type{
            SqlStatement::ChangeDatabase => {
                if let Some(db) = &sql_table_info[0].database{
                    my_cur_db= db.clone();
                }
            }
            _ => {}
        }

        CheckPrivileges{
            cur_db: my_cur_db,
            cur_sql_table_info: sql_table_info,
            sql_type: sql_type.clone(),
            user_name: user_name.clone(),
            host: host.clone(),
            platform: platform.clone()
        }
    }

    /// 检查用户操作权限
    pub async fn check_user_privileges(&self, user_privileges: &AllUserPri) -> Result<()>{
        let all_pri_read_lock = user_privileges.all_pri.read().await;
        for user_pri in &*all_pri_read_lock{
            //首先检查用户是否存在权限
            if user_pri.check_user_name(&self.user_name, &self.platform) {
                'a: for tble_info in &self.cur_sql_table_info{
                    if self.check_information_schema(tble_info).await{
                        continue 'a;
                    }
                    user_pri.check_privileges(self, &tble_info).await?;
                }
                return Ok(());
            }
        }
        let err = format!("Access denied for user '{}'@'{}' ....!", &self.user_name, &self.host);
        return Err(Box::new(MyError(err.into())));
    }

    fn check_cur_sql_table_info(&self, db: &String, table: &String, tbl_info: &TableInfo) -> bool{
        if let Some(my_db) = &tbl_info.database{
            if my_db == db {
                // table为空表示为use、create、drop 语句
                if let Some(t) = &tbl_info.table{
                    if table == t{
                        return true;
                    }
                }else {
                    match self.sql_type {
                        SqlStatement::ChangeDatabase | SqlStatement::Create | SqlStatement::Drop => {
                            return true;
                        }
                        _ => {
                            return false;
                        }
                    }
                }
            }
        }else {
            if &self.cur_db == db{
                if let Some(t) = &tbl_info.table{
                    if table == t{
                        return true;
                    }
                }
            }
        }
        return false;
    }

    async fn check_information_schema(&self, tbl_info: &TableInfo) -> bool{
        return if let Some(db) = &tbl_info.database {
            if db == &String::from("information_schema") {
                true
            } else {
                false
            }
        } else {
            if &self.cur_db == &String::from("information_schema") {
                true
            } else {
                false
            }
        }
        //
        // for tble_all in &self.cur_sql_table_info{
        //     if let Some(db) = &tble_all.db{
        //         if db == &String::from("information_schema"){
        //             continue;
        //         }else {
        //             return false;
        //         }
        //     }else {
        //         if &self.cur_db == &String::from("information_schema"){
        //             continue;
        //         }else {
        //             return false;
        //         }
        //     }
        // }
        //
        // if &self.cur_db == &String::from("information_schema"){
        //     return true;
        // }
        // return false;
    }
}


/// 匹配地址
fn check_host(cur_host: &String, pri_host: &String) -> bool{
    if cur_host == pri_host{
        return true;
    }
    let mut cur_host_chars = cur_host.chars();
    let mut pri_host_chars = pri_host.chars();
    loop{
        if let Some(a) = cur_host_chars.next(){
            if let Some(b) = pri_host_chars.next(){
                if a == b{
                    continue
                }else {
                    if format!("{}",b) == "%".to_string(){
                        return true;
                    }
                    return false;
                }
            }else {
                return false;
            }
        }else {
            return false;
        }
    }
}