mod components;

use components::eb_client::EventBusClient;
use components::thread_pool::ThreadPool;
use serde_json::Value;
use std::sync::Arc;

fn main() -> Result<(), std::io::Error> {
    if let Ok(mut eb_client) =
        EventBusClient::new(std::net::SocketAddr::from(([127, 0, 0, 1], 7000)), 4)
    {
        eb_client
            .register(
                "welcome".to_owned(),
                Arc::new(|msg: Value| {
                    println!("{msg:?}");
                }),
            )?
            .start_listening();
        eb_client.stop_listening();
    };
    Ok(())
}
