use crate::ThreadPool;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Result, Write};
use std::net::{SocketAddr, TcpStream};

struct EventBusClient<T> {
    read_stream: TcpStream,
    write_stream: TcpStream,
    handlers: HashMap<String, T>,
    thread_pool: ThreadPool,
}

impl<T> EventBusClient<T>
where
    T: FnOnce() + Send + 'static,
{
    /// Returns a new __EventBusClient__ connected to the given Socket address.
    ///
    /// **worker_count** determines the thread count of the thread pool which will
    /// process the Event bus messages.
    pub fn new(socket: SocketAddr, worker_count: usize) -> Result<Self> {
        let read_stream = TcpStream::connect(socket)?;
        let write_stream = read_stream.try_clone()?;
        Ok(Self {
            read_stream,
            write_stream,
            handlers: HashMap::new(),
            thread_pool: ThreadPool::new(worker_count),
        })
    }

    /// Registers the __EventBusClient__ to the given Event Bus **address**. This function
    /// also expects the **handler** function that will handle the Event Bus messages
    /// received specifically for the given **address**.
    ///
    /// # Mental Note: You can think **address** as a **topic** name in a regular message queue.
    pub fn register(&mut self, address: String, handler: T) -> Result<&Self> {
        match self.write_packet(OutPacket::Register(Message {
            address: address.clone(),
            body: None,
            headers: None,
        })) {
            Ok(_) => {
                self.handlers.insert(address, handler);
                Ok(self)
            }
            Err(e) => Err(e),
        }
    }

    /// Unregisters the __EventBusClient__ from the given Event Bus **address**.
    ///
    /// # Mental Note: You can think **address** as a **topic** name in a regular message queue.
    pub fn unregister(&mut self, address: String) -> Result<&Self> {
        match self.write_packet(OutPacket::Unregister(Message {
            address: address.clone(),
            body: None,
            headers: None,
        })) {
            Ok(_) => {
                self.handlers.remove(&address);
                Ok(self)
            }
            Err(e) => Err(e),
        }
    }

    /// Writes the given TCP packet to the **write_stream** of the __EventBusClient__.
    fn write_packet(&mut self, packet: OutPacket) -> Result<()> {
        let msg_str = serde_json::to_string(&packet).unwrap();

        // Create a buffer with a capacity of 4 (content legnth) + N (message string length)
        let mut buf = BytesMut::with_capacity(4 + msg_str.len());

        // Fill the buffer starting with the content length value
        buf.put_u32(msg_str.len() as u32);
        buf.put(msg_str.as_bytes());

        // Write to tcp stream
        self.write_stream.write_all(&buf).unwrap();
        self.write_stream.flush()
    }
}

/// Enum representing the out-going TCP packets. Any TCP packet belonging to this Enum
/// meant to be sent **from** the __Event Bus client__ **to** the __remote Event Bus__.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all(serialize = "lowercase", deserialize = "lowercase"))]
#[serde(tag = "type")]
enum OutPacket {
    Ping,
    Register(Message),
    Unregister(Message),
    Send(Message),
    Publish(Message),
}

/// Enum representing the in-coming TCP packets. Any TCP packet belonging to this Enum
/// meant to be sent **from** the __remote Event Bus__ **to** the __Event Bus client__.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all(serialize = "lowercase", deserialize = "lowercase"))]
#[serde(tag = "type")]
enum InPacket {
    #[serde(rename = "err")]
    Error(Message),
    Message(Message),
    Pong,
}

/// Struct representing the Event Bus message content.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Message {
    address: String,
    body: Option<Value>,
    headers: Option<HashMap<String, String>>,
}
