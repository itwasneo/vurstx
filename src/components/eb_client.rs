use crate::ThreadPool;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Result, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};

/// Type representing the **handler** functions that process the incoming
/// Message value.
///
/// __Handler__ is a function wrapped with an Arc pointer so that it could be cloned safely to send
/// to other threads. It takes __serde_json::Value__ and return __nothing__.
type Handler = Arc<dyn Fn(Value) + Send + Sync + 'static>;

/// Struct representing the TCP Client for the Vertx Eventbus Bridge.
///
/// It keeps seperate TcpStream clones for its read and write operations.
///
/// It keeps handler functions for corresponding **registered address** messages in a HashMap. The
/// key values of the HashMap are the **address**es and the values are the __Handler__ functions.
pub struct EventBusClient {
    read_stream: TcpStream,
    write_stream: TcpStream,
    // Handlers HashMap should be stored behind Arc<Mutex> to be sent and modified between threads.
    handlers: Arc<Mutex<HashMap<String, Handler>>>,
    worker_count: usize,
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
        let handlers = Arc::new(Mutex::new(HashMap::new()));
        Ok(Self {
            read_stream,
            write_stream,
            handlers,
            worker_count,
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
                self.handlers.lock().unwrap().insert(address, handler);
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
                self.handlers.lock().unwrap().remove(&address);
                Ok(self)
            }
            Err(e) => Err(e),
        }
    }

    // TODO:
    // Implement unregister_all => this function will unregister the EventBusClient from all the
    // registered **address**es

    /// Sends the given JSON value as message to the given address
    pub fn send(&mut self, address: String, message: Value) {
        self.write_packet(OutPacket::Send(Message {
            address,
            body: Some(message),
            headers: None,
        }))
        .map_err(|e| eprintln!("error occured: {e}"))
        .unwrap();
    }

    /// Publishes the given JSON value as message to the given address
    pub fn publish(&mut self, address: String, message: Value) {
        self.write_packet(OutPacket::Publish(Message {
            address,
            body: Some(message),
            headers: None,
        }))
        .map_err(|e| eprintln!("error occured: {e}"))
        .unwrap();
    }

    /// Starts __EventBusClient__ to listen incoming Event Bus messages belonging to the **address*es
    /// that are previously registered by the client.
    ///
    /// It creates a thread pool which is reposible for processing incoming messages.
    pub fn start_listening(&mut self) {
        if let ClientState::Connected = &mut self.state {
            *&mut self.state = ClientState::Listening;
            let thread_pool = ThreadPool::new(self.worker_count);
            let mut read_stream = self.read_stream.try_clone().unwrap();
            let handlers = Arc::clone(&self.handlers);

            // Here it spawns a new thread and sends the thread pool and a clone of __Handler__s
            // HashMap into the closure. The loop inside the closure can be broken by shutting down
            // the TCP connection.
            std::thread::spawn(move || {
                loop {
                    let mut content_length_buffer = [0u8; 4];
                    match read_stream.read_exact(&mut content_length_buffer) {
                        Ok(_) => {
                            let content_length =
                                u32::from_be_bytes(content_length_buffer.try_into().unwrap());
                            let mut packet_buffer = BytesMut::zeroed(content_length as usize);
                            match read_stream.read_exact(&mut packet_buffer) {
                                Ok(_) => {
                                    // Here deserialization can not be sent to the thread pool due
                                    // to the fact that right now TCP read_stream is **not** behind
                                    // Arc<Mutex> so that it could be sent to other threads.
                                    match serde_json::from_slice(&packet_buffer).unwrap() {
                                        InPacket::Message(m) => {
                                            let handler = Arc::clone(
                                                handlers.lock().unwrap().get(&m.address).unwrap(),
                                            );
                                            thread_pool.execute(move || {
                                                handler(m.body.clone().unwrap());
                                            });
                                        }
                                        InPacket::Error(m) => eprintln!("{m:?}"),
                                        InPacket::Pong => println!("PONG"),
                                    }

                                    let msg: InPacket =
                                        serde_json::from_slice(&packet_buffer).unwrap();
                                    if let InPacket::Message(m) = msg {
                                        let handler = Arc::clone(
                                            handlers.lock().unwrap().get(&m.address).unwrap(),
                                        );
                                        thread_pool.execute(move || {
                                            handler(m.body.clone().unwrap());
                                        });
                                    }
                                }
                                Err(_e) => break,
                            }
                        }
                        Err(_e) => break,
                    }
                }
            });
        }
    }

    /// Stops __EventBusClient__ to listen incoming Event Bus messages entirely by shutting down
    /// the TCP connection. This makes the thread pool that reads the __read_stream__ fail and
    /// break out from the infinite loop.
    pub fn stop_listening(&mut self) {
        if let ClientState::Listening = &mut self.state {
            *&mut self.state = ClientState::Stopped;

            // IMPORTANT:
            // Care here. Right now this function closes the both read and write half of the TCP
            // connection to prevent any connection error that can occur in the server side when
            // the client is closed forecefully from the terminal. This should change when the
            // graceful shutdown is implemented.
            self.read_stream.shutdown(std::net::Shutdown::Both).unwrap();
            println!("Stop Called");
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
pub enum InPacket {
    #[serde(rename = "err")]
    Error(Message),
    Message(Message),
    Pong,
}

/// Struct representing the Event Bus message content.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Message {
    address: String,
    body: Option<Value>,
    headers: Option<HashMap<String, String>>,
}

/// Enum representing the possible __EventBusClient__ status.
#[derive(PartialEq, Debug)]
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

    // TODO:
    // test for invalid content length
    // test for not matching content length and content
    // test for server goes down and up again / some sort of pingin mechanism may be needed
}
