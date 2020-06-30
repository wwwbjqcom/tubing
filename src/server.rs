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
use std::str::from_utf8;
use crate::{Config, Result, readvalue};
use crate::dbengine;
use crate::dbengine::client::{ClientResponse};
use crate::dbengine::server::HandShake;
pub mod shutdown;
mod connection;
mod per_connection;
pub mod sql_parser;
use connection::Connection;
use tracing_subscriber::util::SubscriberInitExt;
use crate::mysql::pool::{ConnectionsPool, MysqlConnectionInfo};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use crate::dbengine::{SERVER_STATUS_AUTOCOMMIT, SERVER_STATUS_IN_TRANS, CLIENT_BASIC_FLAGS, CLIENT_PROTOCOL_41};
use crate::mysql::connection::PacketHeader;

pub async fn run(listener: TcpListener , config: Arc<Config>,  mysql_pool: ConnectionsPool, shutdown: impl Future) -> crate::Result<()> {
    // A broadcast channel is used to signal shutdown to each of the active
    // connections. When the provided `shutdown` future completes
    let (notify_shutdown, _) = broadcast::channel(1);
    let (notify_shutdown_t, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let (shutdown_complete_tx_t, shutdown_complete_rx_t) = mpsc::channel(1);

    let mut pool_maintain = ThreadPoolMaintain{
        pool: mysql_pool.clone(),
        notify_shutdown_t,
        shutdown_complete_rx_t,
        shutdown_complete_tx_t
    };


    // Initialize the listener state
    let mut server = Listener {
        pool: mysql_pool,
        config,
        listener,
        limit_connections: Arc::new(Semaphore::new(crate::MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };
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
                error!("thread pool maintain faild");
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

    let ThreadPoolMaintain {
        mut shutdown_complete_rx_t,
        shutdown_complete_tx_t,
        ..
    } = pool_maintain;

    drop(shutdown_complete_tx);
    drop(shutdown_complete_tx_t);
    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;
    let _ = shutdown_complete_rx_t.recv().await;
    Ok(())
}


struct ThreadPoolMaintain{
    /// Mysql thread pool
    pool: ConnectionsPool,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown_t: broadcast::Sender<()>,

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
    shutdown_complete_rx_t: mpsc::Receiver<()>,
    shutdown_complete_tx_t: mpsc::Sender<()>,
}

impl ThreadPoolMaintain{
    async fn run(&mut self) -> Result<()> {
//        let mut shutdown = shutdown::Shutdown::new(self.notify_shutdown_t.subscribe());
//        while !shutdown.is_shutdown(){
//            println!("abc");
//            tokio::select! {
//                _ = self.pool.check_health() => {
//                    error!("thread pool maintain faild");
//                },
//                _ = shutdown.recv() => {
//                    // If a shutdown signal is received, return from `run`.
//                    // This will result in the task terminating.
//                    return Ok(());
//                }
//            };
//        }
        self.pool.check_health().await?;
        info!("abc");
        Ok(())
    }
}

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener {
    /// Mysql thread pool
    pool: ConnectionsPool,

    /// Server user password configuration
    config: Arc<Config>,

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

            // Create the necessary per-connection handler state.
            let mut handler = Handler {
                platform: None,
                per_conn_info: per_connection::PerMysqlConn::new(),
                hand_key: thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(32)
                    .collect(),
                auto_commit: false,
                commited: false,
                client_flags: 0,
                pool: self.pool.clone(),
                db: Some("information_schema".to_string()),
                seq: 0,
                config: self.config.clone(),

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
            let mut pool_clone = self.pool.clone();
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run().await {
                    pool_clone.active_count_sub();
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
    ///
    Sleep,
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
    
    pub per_conn_info: per_connection::PerMysqlConn,

    pub hand_key: String,

    pub auto_commit: bool,
    
    pub commited: bool,

    pub client_flags: i32,

    /// mysql thread pool
    pub pool: ConnectionsPool,

    /// current used database
    /// default information_schema
    pub db: Option<String>,

    /// current sequence id
    pub seq: u8,

    /// Server user/password configuration
    config: Arc<Config>,

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

        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            match &self.status {
                // first handshake verification
                ConnectionStatus::Null => {
                    self.handshake().await?;
                    continue;
                }
                // the handshake verification fails, the terminal connects
                ConnectionStatus::Failure => {
                    info!("handshake failed.");
                    return Ok(())
                }
                ConnectionStatus::Quit => {
                    return Ok(())
                }
                _ => {}
            }
            let maybe_response = tokio::select! {
                res = self.connection.read(&self.seq) => res?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
                _ = self.per_conn_info.health(&mut self.pool) => {
                    continue;
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let response = match maybe_response {
                Some(response) => response,
                None => return Ok(()),
            };

            if !self.check_seq(&response.seq){
                return Ok(())
            }
            match &self.status {
                ConnectionStatus::Auth(handshake) => {
                    let (buf, db, flags)= handshake.auth(&response, &self.config, self.get_status_flags()).await?;
                    if &buf[0] == &0{
                        self.status = ConnectionStatus::Connected;
                        self.client_flags = flags;
                        self.db = db;
                    }else {
                        self.status = ConnectionStatus::Failure;
                    }
                    self.send(&buf).await?;
                    self.reset_seq();
                }
                ConnectionStatus::Connected => {
//                    let a = vec![00,00,00,01,00,00,00];
//                    self.connection.send(&a, &self.seq).await?;
//                    println!("{:?}", response);
                    response.exec(&mut self).await?;
                    self.reset_seq();
                }
                _ => {}
            }
        }
        self.per_conn_info.return_connection().await?;
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

    /// operate client requests
    ///
    /// after the client handshake is successful,
    /// process and reply to the received request
    async fn response_operation(&mut self, buf: &ClientResponse) -> Result<()> {
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

    pub fn get_status_flags(&self) -> u16 {
        if self.auto_commit{
            return (SERVER_STATUS_AUTOCOMMIT | SERVER_STATUS_IN_TRANS) as u16;
        }else {
            return SERVER_STATUS_IN_TRANS as u16;
        }
    }

    pub async fn set_per_conn_cached(&mut self) -> Result<()> {
        if let Some(conn) = &mut self.per_conn_info.conn_info{
            conn.set_cached(&self.hand_key).await?;
        }
        Ok(())
    }

    /// 发送error packet
    pub async fn send_error_packet(&mut self, error: &String) -> Result<()>{
        let mut err = vec![];
        err.push(0xff);
        err.extend(readvalue::write_u16(10));
        if CLIENT_BASIC_FLAGS & CLIENT_PROTOCOL_41 > 0{
            for _ in 0..5{
                err.push(0);
            }
        }
        err.extend(error.as_bytes());
        self.send(&err).await?;
        self.reset_seq();
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

