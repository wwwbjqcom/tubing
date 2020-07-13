/*
@author: xiao cai niao
@datetime: 2020/7/13
*/

use reqwest;
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use crate::{MyConfig, mysql, MyError};

#[derive(Serialize)]
pub struct GetRouteInfo {
    pub hook_id: String,
    pub clusters: Vec<String>,
}
impl GetRouteInfo{
    fn new(conf: &MyConfig) -> mysql::Result<GetRouteInfo>{
        let mut tmp = GetRouteInfo{ hook_id: "".to_string(), clusters: vec![] };
        if let Some(hook_id) = &conf.hook_id{
            tmp.hook_id = hook_id.clone();
            match &conf.cluster{
                Some(v) => {
                    if v.len()> 0{
                        tmp.clusters = v.clone();
                    }else {
                        let err = String::from("cluster list can not be empty");
                        return Err(Box::new(MyError(err.into())));
                    }
                }
                None => {
                    let err = String::from("cluster list can not be empty");
                    return Err(Box::new(MyError(err.into())));
                }
            }
            return Ok(tmp);
        }
        let err = String::from("hook id can not be empty");
        return Err(Box::new(MyError(err.into())));
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MysqlHostInfo {
    pub host: String,
    pub port: usize
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RouteInfo {
    pub cluster_name: String,
    pub write: MysqlHostInfo,
    pub read: Vec<MysqlHostInfo>
}

impl RouteInfo{
    pub fn get_write_host_info(&self) -> String{
        return format!("{}:{}", self.write.host, self.write.port);
    }
    pub fn get_read_host_info(&self) -> Option<Vec<String>>{
        let mut read_host_info = vec![];
        for host_info in &self.read{
            read_host_info.push(format!("{}:{}", host_info.host, host_info.port));
        }
        if read_host_info.len()>0{
            return Some(read_host_info);
        }else {
            return None;
        }

    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseRouteInfo {
    pub route: Vec<RouteInfo>
}

#[derive(Serialize, Debug, Deserialize)]
pub struct ResponseValue{
    pub status: u8,
    pub value: ResponseRouteInfo
}

pub async fn get_platform_route(conf: &MyConfig) -> mysql::Result<ResponseValue> {
    let map = json!(GetRouteInfo::new(conf)?);
    let client = reqwest::Client::new();
    if let Some(url) = &conf.server_url{
        let res = client.post(url)
            .json(&map)
            .send()
            .await?
            .json::<ResponseValue>()
            .await?;
        return Ok(res)
    }
    let err = String::from("connection to ha_server error");
    return Err(Box::new(MyError(err.into())));
}