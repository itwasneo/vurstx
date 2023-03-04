mod components;

use components::eb_client::EventBusClient;
use components::thread_pool::ThreadPool;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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

fn main() -> Result<(), std::io::Error> {
    if let Ok(mut eb_client) =
        EventBusClient::new(std::net::SocketAddr::from(([127, 0, 0, 1], 7000)), 4)
    {
        eb_client
            .register("welcome".to_owned(), Box::new(|| {}))?
            .start_listening();
        eb_client.stop_listening();
    };
    Ok(())
}
