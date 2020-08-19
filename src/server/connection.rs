/*
@author: xiao cai niao
@datetime: 2020/5/14
*/
use crate::mysql::Result;
use crate::dbengine::client;
use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use byteorder::{WriteBytesExt, LittleEndian};
use tracing::{debug};


/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub(crate) struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufStream<TcpStream>,

    // The buffer for reading frames. Unfortunately, Tokio's `BufReader`
    // currently requires you to empty its buffer before you can ask it to
    // retrieve more data from the underlying stream, so we have to manually
    // implement buffering. This should be fixed in Tokio v0.3.
    buffer: BytesMut
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub(crate) fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufStream::new(socket),
            // Default to a 64MB read buffer. For the use case of mysql server max packet,
            buffer: BytesMut::with_capacity(64 * 1024 * 1024)
        }
    }

    /// read the data stream from the connection
    pub async fn read(&mut self) -> Result<Option<client::ClientResponse>> {
        loop {
            let mut buf = Cursor::new(&self.buffer[..]);
            if self.check_data(&mut buf)? {
                debug!("{}",crate::info_now_time(String::from("get response from client start")));
                let response = client::ClientResponse::new(&mut buf).await?;
                debug!("read response success. payload: {:?}, larger_len: {}....", &response.payload, &response.larger.len());
                if response.payload > 0{
                    // The `client::ClientResponse::new` function will have advanced the cursor until
                    // the end of the packet. Since the cursor had position set
                    // to zero before `client::ClientResponse::new` was called, we obtain the
                    // length of the packet by checking the cursor position.
                    let len = buf.position() as usize;
                    // Reset the position to zero before passing the cursor to
                    // `client::ClientResponse::new`.
                    buf.set_position(0);
                    // Discard the parsed data from the read buffer.
                    //
                    // When `advance` is called on the read buffer, all of the
                    // data up to `len` is discarded. The details of how this
                    // works is left to `BytesMut`. This is often done by moving
                    // an internal cursor, but it may be done by reallocataing
                    // and copying data.
                    self.buffer.advance(len);
                    return Ok(Some(response));
                }
            }

            debug!("{}",crate::info_now_time(String::from("get response from client")));
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }

    }

    fn check_data(&self, src: &mut Cursor<&[u8]>) -> Result<bool> {
        if !src.has_remaining() {
            //return Err(Box::new(MyError(String::from("no data").into())));
            return Ok(false)
        }

        Ok(true)
    }

    /// send a packet to the connection
    pub async fn send(&mut self, packet: &Vec<u8>, seq_id: &u8) -> io::Result<()> {
        let packet_all = self.packet_value(packet, seq_id);
        self.stream.write_all(&packet_all).await?;
        self.stream.flush().await
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }

    /// send a packet to the connection
    pub async fn send_packet_full(&mut self, packet: &Vec<u8>) -> io::Result<()> {
        debug!("{}",crate::info_now_time(String::from("start write all to client")));
        self.stream.write_all(&packet).await
//        debug!("{}",crate::info_now_time(String::from("flush to client")));
//        self.stream.flush().await
    }


    fn packet_value(&self, packet: &Vec<u8>, seq_id: &u8) -> Vec<u8> {
        let mut packet_all: Vec<u8> = vec![];
        let payload = self.write_u24(packet.len() as u32);
        packet_all.extend(payload);
        packet_all.push(seq_id.clone());
        packet_all.extend(packet);
        return packet_all;
    }

    fn write_u24(&self, num: u32) -> Vec<u8> {
        let mut rdr = Vec::new();
        rdr.write_u24::<LittleEndian>(num).unwrap();
        return rdr;
    }
}