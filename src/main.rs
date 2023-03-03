mod components;

use bytes::{BufMut, BytesMut};
use components::thread_pool::ThreadPool;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all(serialize = "lowercase", deserialize = "lowercase"))]
#[serde(tag = "type")]
enum Packet {
    Ping,
    Pong,
    Register(Message),
    Send(Message),
    Message(Message),
    Publish(Message),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Message {
    address: String,
    body: Option<Value>,
    headers: Option<HashMap<String, String>>,
}

fn main() {
    //let listener = TcpListener::bind("127.0.0.1:7000").unwrap();
    let pool = ThreadPool::new(4);
    let mut proccessed_message_count = 0;
    if let Ok(stream) = TcpStream::connect("127.0.0.1:7000") {
        println!("Connected to the server");
        let mut read_stream = stream.try_clone().unwrap();

        write_packet_to_tcp_stream(
            &stream,
            Packet::Register(Message {
                address: "welcome".to_owned(),
                body: None,
                headers: None,
            }),
        )
        .unwrap();

        loop {
            let mut content_length_buffer = [0u8; 4];
            match read_stream.read_exact(&mut content_length_buffer) {
                Ok(_) => {
                    let content_length =
                        u32::from_be_bytes(content_length_buffer.try_into().unwrap());
                    let mut packet_buffer = BytesMut::zeroed(content_length as usize);
                    match read_stream.read_exact(&mut packet_buffer) {
                        Ok(_) => {
                            pool.execute(move || process_packet_buffer(&packet_buffer));
                            proccessed_message_count += 1;
                        }
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            }
        }
    } else {
        println!("Not connected to the server")
    }
    println!("{proccessed_message_count}");
}

fn write_packet_to_tcp_stream(mut stream: &TcpStream, packet: Packet) -> std::io::Result<()> {
    let msg_str = serde_json::to_string(&packet).unwrap();

    // Create a buffer with a capacity of 4 (content legnth) + N (message string length)
    let mut buf = BytesMut::with_capacity(4 + msg_str.len());

    // Fill the buffer starting with the content length value
    buf.put_u32(msg_str.len() as u32);
    buf.put(msg_str.as_bytes());

    // Write to tcp stream
    stream.write_all(&buf).unwrap();
    stream.flush()
}

fn read_packet_from_tcp_stream(mut stream: &TcpStream) -> std::io::Result<()> {
    let buf_reader = BufReader::new(&mut stream);
    let buffer = buf_reader.buffer();
    let (content_length, packet) = buffer.split_at(std::mem::size_of::<u32>());
    let content_length = u32::from_be_bytes(content_length.try_into().unwrap());
    if content_length == packet.len() as u32 {
        println!("{:?}", packet);
        stream.flush()
    } else {
        stream.flush().unwrap();
        panic!("INVALID PACKET");
    }
}

fn process_packet_buffer(packet: &[u8]) {
    let msg: Packet = serde_json::from_slice(packet).unwrap();
    println!("{msg:?}");
}

#[cfg(test)]
mod tests {
    use super::{Message, Packet};
    use bytes::{BufMut, BytesMut};

    const JSON_PING: &str = r#"{"type":"ping"}"#;
    const JSON_PONG: &str = r#"{"type":"pong"}"#;
    const JSON_REGISTER: &str = r#"{"type":"register", "address":"address"}"#;
    const JSON_MESSAGE: &str = r#"{"type":"message", "address":"address"}"#;

    #[test]
    fn message_serde_tests() {
        assert_eq!(Packet::Ping, serde_json::from_str(JSON_PING).unwrap());
        assert_eq!(Packet::Pong, serde_json::from_str(JSON_PONG).unwrap());
        assert_eq!(
            Packet::Register(Message {
                address: "address".to_owned(),
                body: None,
                headers: None,
            }),
            serde_json::from_str(JSON_REGISTER).unwrap()
        );
        assert_eq!(
            Packet::Send(Message {
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
