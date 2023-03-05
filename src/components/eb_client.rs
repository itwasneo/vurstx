use crate::ThreadPool;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Result, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

type Handler = Arc<dyn Fn(Value) + Send + Sync + 'static>;

pub struct EventBusClient {
    read_stream: TcpStream,
    write_stream: TcpStream,
    handlers: HashMap<String, Handler>,
    thread_pool: ThreadPool,
    state: ClientState,
}

impl EventBusClient {
    /// Returns a new __EventBusClient__ connected to the given Socket address.
    ///
    /// **worker_count** determines the thread count of the thread pool which will
    /// process the Event bus messages.
    pub fn new(socket: SocketAddr, worker_count: usize) -> Result<Self> {
        let read_stream = TcpStream::connect(socket)?;
        let write_stream = read_stream.try_clone()?;
        let handlers: HashMap<String, Handler> = HashMap::new();
        Ok(Self {
            read_stream,
            write_stream,
            handlers,
            thread_pool: ThreadPool::new(worker_count),
            state: ClientState::Connected,
        })
    }

    /// Registers the __EventBusClient__ to the given Event Bus **address**. This function
    /// also expects the **handler** function that will handle the Event Bus messages
    /// received specifically for the given **address**.
    ///
    /// # Mental Note:
    /// You can think **address** as a **topic** name in a regular message queue.
    pub fn register(&mut self, address: String, handler: Handler) -> Result<&mut Self> {
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
    /// # Mental Note:
    /// You can think **address** as a **topic** name in a regular message queue.
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

    /// Starts __EventBusClient__ to listen incoming Event Bus messages belonging to the **address*es
    /// that are previously registered by the client.
    ///
    /// As long as the **state** value of the __EventBusClient__ is **Listening**, the client keeps
    /// listening to the Event Bus messages.
    pub fn start_listening(&mut self) {
        if self.state == ClientState::Connected {
            self.state = ClientState::Listening;
            while self.state == ClientState::Listening {
                let mut content_length_buffer = [0u8; 4];
                match self.read_stream.read_exact(&mut content_length_buffer) {
                    Ok(_) => {
                        let content_length =
                            u32::from_be_bytes(content_length_buffer.try_into().unwrap());
                        let mut packet_buffer = BytesMut::zeroed(content_length as usize);
                        match self.read_stream.read_exact(&mut packet_buffer) {
                            Ok(_) => {
                                let msg: InPacket = serde_json::from_slice(&packet_buffer).unwrap();
                                if let InPacket::Message(m) = msg {
                                    let handler =
                                        Arc::clone(self.handlers.get(&m.address).unwrap());
                                    self.thread_pool.execute(move || {
                                        handler(m.body.clone().unwrap());
                                    });
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }

    /// Stops __EventBusClient__ to listen incoming Event Bus messages entirely by changing the
    /// client's **state** value.
    pub fn stop_listening(&mut self) {
        self.state = ClientState::Stopped;
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
pub enum InPacket {
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

#[derive(PartialEq)]
enum ClientState {
    Connected,
    Listening,
    Stopped,
}

#[cfg(test)]
mod tests {
    use super::{InPacket, Message, OutPacket};
    use bytes::{BufMut, BytesMut};

    const JSON_PING: &str = r#"{"type":"ping"}"#;
    const JSON_PONG: &str = r#"{"type":"pong"}"#;
    const JSON_REGISTER: &str = r#"{"type":"register", "address":"address"}"#;
    const JSON_MESSAGE: &str = r#"{"type":"message", "address":"address"}"#;

    #[test]
    fn message_serde_tests() {
        assert_eq!(OutPacket::Ping, serde_json::from_str(JSON_PING).unwrap());
        assert_eq!(InPacket::Pong, serde_json::from_str(JSON_PONG).unwrap());
        assert_eq!(
            OutPacket::Register(Message {
                address: "address".to_owned(),
                body: None,
                headers: None,
            }),
            serde_json::from_str(JSON_REGISTER).unwrap()
        );
        assert_eq!(
            OutPacket::Send(Message {
                address: "address".to_owned(),
                body: None,
                headers: None
            }),
            serde_json::from_str(JSON_MESSAGE).unwrap()
        );
    }

    #[test]
    fn adding_content_length_test() {
        let mut buf = BytesMut::with_capacity(4 + JSON_PONG.len());
        buf.put_u32(JSON_PONG.len() as u32);
        buf.put(JSON_PONG.as_bytes());
        assert_eq!(b"\0\0\0\x0f{\"type\":\"pong\"}"[..], buf);
    }
}
