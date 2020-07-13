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
        println!("{:?}", conf);
        let mut tmp = GetRouteInfo{ hook_id: "".to_string(), clusters: vec![] };
        if let Some(hook_id) = &conf.hook_id{
            tmp.hook_id = hook_id.clone();
            println!("{:?}",&conf.cluster);
            match &conf.cluster{
                Some(v) => {
                    if v.len()> 0{
                        tmp.clusters = v.clone();
                    }
                    tmp.clusters = vec![String::from("all")];
                }
                None => {
                    tmp.clusters = vec![String::from("all")];
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

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseRouteInfo {
    pub route: Vec<RouteInfo>
}

#[derive(Serialize, Debug, Deserialize)]
pub struct ResponseValue{
    status: u8,
    value: ResponseRouteInfo
}

pub async fn get_platform_route(conf: &MyConfig) -> mysql::Result<()> {
    let map = json!(GetRouteInfo::new(conf)?);
    println!("{:?}", &map);
    let client = reqwest::Client::new();
    if let Some(url) = &conf.server_url{
        let res = client.post(url)
            .json(&map)
            .send()
            .await?
            .json::<ResponseValue>()
            .await?;
        println!("{:?}", &res);
    }
    Ok(())
}