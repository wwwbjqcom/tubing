/*
@author: xiao cai niao
@datetime: 2020/5/14
*/

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};
use num_cpus;
use crate::{MyConfig, mysql};
use crate::mysql::Result;
use crate::mysql::pool::{PlatformPool, ConnectionsPoolPlatform};
use crate::dbengine;
use crate::MyError;
use crate::dbengine::server::HandShake;
pub mod shutdown;
mod connection;
mod per_connection;
pub mod mysql_mp;
pub mod sql_parser;
use connection::Connection;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use crate::dbengine::{SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_IN_TRANS};
use tokio::runtime::Builder;
use crate::server::mysql_mp::ResponseValue;
use crate::mysql::privileges::AllUserPri;

pub fn run(mut config: MyConfig, shutdown: impl Future) -> Result<()> {
    // A broadcast channel is used to signal shutdown to each of the active
    // connections. When the provided `shutdown` future completes
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let cpus = num_cpus::get();
    let mut runtime = Builder::new()
        .threaded_scheduler()
        .core_threads(cpus * 4)
        .max_threads(100)
        .enable_all()
        .thread_name("my-custom-name")
        .thread_stack_size(3 * 1024 * 1024)
        .build()
        .unwrap();

    // Concurrently run the server and listen for the `shutdown` signal. The
    // server task runs until an error is encountered, so under normal
    // circumstances, this `select!` statement runs until the `shutdown` signal
    // is received.
    //
    // `select!` statements are written in the form of:
    //
    // ```
    // <result of async op> = <async op> => <step to perform with result>
    // ```
    //
    // All `<async op>` statements are executed concurrently. Once the **first**
    // op completes, its associated `<step to perform with result>` is
    // performed.
    //
    // The `select! macro is a foundational building block for writing
    // asynchronous Rust. See the API docs for more details:
    //
    // https://docs.rs/tokio/*/tokio/macro.select.html
    runtime.block_on(async{

        //通过高可用远程获取各业务集群路由关系并初始化配置
        if config.check_is_mp(){
            let ha_route: ResponseValue  = mysql_mp::get_platform_route(&config).await?;
            debug!("get_platform_route: {:?}", &ha_route);
            config.reset_init_config(&ha_route);
        }

        //创建各业务后端连接池
        let (platform_pool, all_user_info) = mysql::pool::PlatformPool::new(&config)?;
        debug!("init_config: {:?}", &platform_pool.platform_node_info);
        let mut user_pri = AllUserPri::new(&platform_pool);
        user_pri.get_pris(&all_user_info).await?;

        let mut port: u16 = crate::DEFAULT_PORT.parse().unwrap();
        if let Some(l_port) = config.port{
            port = l_port;
        };
        let listener = TcpListener::bind(&format!("{}:{}",config.bind, port)).await?;

        let mut pool_maintain = ThreadPoolMaintain{
            platform_pool: platform_pool.clone(),
        };


        // Initialize the listener state
        let mut server = Listener {
            platform_pool,
            user_privileges: user_pri,
            listener,
            limit_connections: Arc::new(Semaphore::new(crate::MAX_CONNECTIONS)),
            notify_shutdown,
            shutdown_complete_tx,
            shutdown_complete_rx,
        };

        tokio::select! {
            res = server.run() => {
                // If an error is received here, accepting connections from the TCP
                // listener failed multiple times and the server is giving up and
                // shutting down.
                //
                // Errors encountered when handling individual connections do not
                // bubble up to this point.
                if let Err(err) = res {
                    error!(cause = %err, "failed to accept");
                }
            }

            a = pool_maintain.run() => {
                if let Err(err) = a {
                    error!("thread pool maintain faild:{:?}",err.to_string());
                }

            }

            _ = shutdown => {
                // The shutdown signal has been received.
                info!("shutting down");
            }
        }

        // Extract the `shutdown_complete` receiver and transmitter
        // explicitly drop `shutdown_transmitter`. This is important, as the
        // `.await` below would otherwise never complete.
        let Listener {
            mut shutdown_complete_rx,
            shutdown_complete_tx,
            ..
        } = server;


        drop(shutdown_complete_tx);
        // Wait for all active connections to finish processing. As the `Sender`
        // handle held by the listener has been dropped above, the only remaining
        // `Sender` instances are held by connection handler tasks. When those drop,
        // the `mpsc` channel will close and `recv()` will return `None`.
        let _ = shutdown_complete_rx.recv().await;

        Ok(())
    })

}


struct ThreadPoolMaintain{
    /// all connections pool
    platform_pool: PlatformPool,
}

impl ThreadPoolMaintain{
    async fn run(&mut self) -> Result<()> {
        self.platform_pool.check_health().await?;
        info!("shutdown");
        Ok(())
    }
}

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// all connections pool
    platform_pool: PlatformPool,

    /// mysql account privileges
    user_privileges: AllUserPri,
    
    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections.
    ///
    /// A `Semaphore` is used to limit the max number of connections. Before
    /// attempting to accept a new connection, a permit is acquired from the
    /// semaphore. If none are available, the listener waits for one.
    ///
    /// When handlers complete processing a connection, the permit is returned
    /// to the semaphore.
    limit_connections: Arc<Semaphore>,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,

}

impl Listener {
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    async fn run(&mut self) -> Result<()> {
        info!("accepting inbound connections");
        loop {
            // Wait for a permit to become available
            //
            // `acquire` returns a permit that is bound via a lifetime to the
            // semaphore. When the permit value is dropped, it is automatically
            // returned to the sempahore. This is convenient in many cases.
            // However, in this case, the permit must be returned in a different
            // task than it is acquired in (the handler task). To do this, we
            // "forget" the permit, which drops the permit value **without**
            // incrementing the semaphore's permits. Then, in the handler task
            // we manually add a new permit when processing completes.
            self.limit_connections.acquire().await.forget();

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let socket = self.accept().await?;
            let host = socket.peer_addr()?.ip().to_string();
            // Create the necessary per-connection handler state.
            let handler = Handler {
                platform: None,
                platform_pool: self.platform_pool.clone(),
                platform_pool_on: ConnectionsPoolPlatform::default(),
                per_conn_info: per_connection::PerMysqlConn::new(),
                hand_key: thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(32)
                    .collect(),
                user_privileges: self.user_privileges.clone(),
                auto_commit: false,
                commited: false,
                client_flags: 0,
                db: Some("information_schema".to_string()),
                host,
                user_name: "".to_string(),
                seq: 0,

                status: ConnectionStatus::Null,

                // Initialize the connection state. This allocates read/write
                // buffers to perform redis protocol frame parsing.
                connection: Connection::new(socket),

                // The connection state needs a handle to the max connections
                // semaphore. When the handler is done processing the
                // connection, a permit is added back to the semaphore.
                limit_connections: self.limit_connections.clone(),

                // Receive shutdown notifcations.
                shutdown: shutdown::Shutdown::new(self.notify_shutdown.subscribe()),

                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),

            };

            // Spawn a new task to process the connections. Tokio tasks are like
            // asynchronous green threads and are executed concurrently.
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }


    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;
        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::delay_for(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}



/// Per-connection state
///
#[derive(Debug)]
pub enum  ConnectionStatus {
    /// connection succeeded
    Connected,
    /// verification phase
    /// when exchanging account password agreement with the server
    Auth(HandShake),
    /// auth switch
    Switch(HandShake),

    ///
    /// authentication failed
    Failure,
    /// initial socket connection
    /// the connection just created is in this state
    Null,

    Quit,
}


/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`.
#[derive(Debug)]
pub struct Handler {
    /// record the business library to which the current connection belongs
    pub platform: Option<String>,

    /// all connections pool
    pub platform_pool: PlatformPool,

    /// all connection pools of the current business platform
    pub platform_pool_on: ConnectionsPoolPlatform,

    /// current connection
    pub per_conn_info: per_connection::PerMysqlConn,

    pub hand_key: String,

    pub auto_commit: bool,
    
    pub commited: bool,

    pub client_flags: i32,

    pub user_privileges: AllUserPri,

    /// current used database
    /// default information_schema
    pub db: Option<String>,

    pub host: String,

    pub user_name: String,
    /// current sequence id
    pub seq: u8,

    /// connection state.
    ///
    /// record the current status of each connection for maintenance
    pub status: ConnectionStatus,

    /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,

    /// Max connection semaphore.
    ///
    /// When the handler is dropped, a permit is returned to this semaphore. If
    /// the listener is waiting for connections to close, it will be notified of
    /// the newly available permit and resume accepting connections.
    limit_connections: Arc<Semaphore>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connction is terminated.
    shutdown: shutdown::Shutdown,

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,

}

impl Handler {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[instrument(skip(self))]
    async fn run(mut self) -> Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        //self.get_platform_conn_on(&"test1".to_string()).await?;
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            match &self.status {
                // first handshake verification
                ConnectionStatus::Null => {
                    debug!("{}",crate::info_now_time(String::from("send auth packet")));
                    self.handshake().await?;
                    continue;
                }
                // the handshake verification fails, the terminal connects
                ConnectionStatus::Failure => {
                    info!("handshake failed.");
                    return Ok(())
                }
                ConnectionStatus::Quit => {
                    self.per_conn_info.return_connection(&mut self.platform_pool_on, self.seq.clone()).await?;
                    return Ok(())
                }
                _ => {}
            }
            debug!("{}",crate::info_now_time(String::from("start read maybe_response")));
            let maybe_response = tokio::select! {
                res = self.connection.read() => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    break;
                }
                _ = self.per_conn_info.health(&mut self.platform_pool_on) => {
                    continue;
                }
            };
            debug!("{}",crate::info_now_time(String::from("read one maybe_response")));
            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let response = match maybe_response {
                Some(response) => response,
                None => break,
            };
            debug!("{:?}", &response);
            debug!("{}",crate::info_now_time(String::from("start")));
            if !self.check_seq(&response.seq){
                break;
            }
            debug!("{}",crate::info_now_time(String::from("check_seq")));
            match &self.status {
                ConnectionStatus::Auth(handshake) => {
                    let (buf, db, flags, user_name, switch)= handshake.auth(&response,
                                                                    self.get_status_flags(),
                                                                    &self.platform_pool).await?;
                    if switch{
                        self.status = ConnectionStatus::Switch(handshake.clone());
                        self.client_flags = flags;
                        self.db = db;
                        self.user_name = user_name;
                        self.send(&buf).await?;
                        self.seq += 1;
                        continue;
                    }

                    if &buf[0] == &0{
                        self.status = ConnectionStatus::Connected;
                        self.client_flags = flags;
                        self.db = db;
                        self.user_name = user_name;
                    }else {
                        self.status = ConnectionStatus::Failure;
                    }
                    self.send(&buf).await?;
                    self.reset_seq();
                }
                ConnectionStatus::Switch(handshake) => {
                    let buf = handshake.switch_auth(&response, &self.platform_pool, &self.user_name, self.get_status_flags()).await?;
                    self.send(&buf).await?;
                    if &buf[0] == &0{
                        self.status = ConnectionStatus::Connected;
                    }else {
                        self.status = ConnectionStatus::Failure;
                    }
                    self.reset_seq();
                }

                ConnectionStatus::Connected => {
                    debug!("{}",crate::info_now_time(String::from("start execute")));
                    if let Err(e) = response.exec(&mut self).await{
                        //发生异常关闭对应db连接，因为如果是语法或sql错误会直接发送到客户端， 这里返回错误一般是mysql异常
                        error!("{}", e.to_string());
                        break;
                    }
                    self.reset_seq();
                }
                _ => {}
            }
        }
        self.per_conn_info.return_connection(&mut self.platform_pool_on, self.seq.clone()).await?;
        Ok(())
    }

    /// the server sends the first handshake packet
    ///
    /// successfully sent, then modify the connection
    /// status value to auth, and set the sequence id
    async fn handshake(&mut self) -> Result<()> {
        let handshake = dbengine::server::HandShake::new();
        let handshake_packet = handshake.server_handshake_packet()?;
        self.send(&handshake_packet).await?;
        self.status = ConnectionStatus::Auth(handshake);
        self.seq += 1;
        Ok(())
    }

    pub async fn send(&mut self, packet: &Vec<u8>) -> Result<()>{
        self.connection.send(packet, &self.seq).await?;
        Ok(())
    }


    pub async fn send_full(&mut self, packet: &Vec<u8>) -> Result<()>{
        self.connection.send_packet_full(packet).await?;
        Ok(())
    }

    pub async fn stream_flush(&mut self) -> Result<()> {
        debug!("{}",crate::info_now_time(String::from("flush to client")));
        Ok(self.connection.flush().await?)
    }

    /// check response seq id
    fn check_seq(&mut self, seq: &u8) -> bool {
        if &self.seq == seq{
            self.seq += 1;
            true
        }else {
            false
        }
    }

    pub fn reset_seq(&mut self) {
        self.seq = 0;
    }

    pub fn seq_add(&mut self) {
        self.seq += 1;
    }

    pub fn get_status_flags(&self) -> u16 {
        if self.auto_commit{
            return (SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS) as u16;
        }else {
            return SERVER_STATUS_IN_TRANS as u16;
        }
    }

    /// 当用户设置platform时，如果权限检查通过则获取对应业务组的总连接池
    pub async fn get_platform_conn_on(&mut self, platform: &String) -> Result<bool> {
        if let Some(pool)  = self.platform_pool.get_platform_pool(platform).await{
            self.platform_pool_on = pool;
            return Ok(true)
        }
        return Ok(false)
    }

    /// 当设置platform时进行检查，如果当前有platform则判断是否相同
    ///
    /// 如果不同则需要归还当前连接并重新获取
    ///
    /// 相同则直接返回
    pub async fn check_cur_platform(&mut self, platform: &String) -> Result<()>{
        if platform == "admin"{
            self.platform = Some(platform.clone());
            return Ok(())
        }
        if let Some(cur_platform) = &self.platform{
            if cur_platform != platform{
                //归还连接
                self.per_conn_info.return_connection(&mut self.platform_pool_on, 0).await?;
                self.check_get_connection(platform).await?;
            }
        }else {
            self.check_get_connection(platform).await?;
        }
        return Ok(());
    }

    async fn check_get_connection(&mut self, platform: &String) -> Result<()> {
        if !self.get_platform_conn_on(platform).await?{
            let error = format!("no available connection for this platform({})", platform);
            error!("{}", &error);
            return Err(Box::new(MyError(error.into())));
            //self.send_error_packet(handler, &error).await?;
        }else {
            self.platform = Some(platform.clone());
        }
        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Add a permit back to the semaphore.
        //
        // Doing so unblocks the listener if the max number of
        // connections has been reached.
        //
        // This is done in a `Drop` implementation in order to guaranatee that
        // the permit is added even if the task handling the connection panics.
        // If `add_permit` was called at the end of the `run` function and some
        // bug causes a panic. The permit would never be returned to the
        // semaphore.
        self.limit_connections.add_permits(1);
    }
}

