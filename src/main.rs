mod components;

use components::eb_client::EventBusClient;
use components::thread_pool::ThreadPool;
use serde_json::Value;
use std::sync::Arc;

fn main() -> Result<(), std::io::Error> {
    if let Ok(mut eb_client) =
        //  Creating an __EventBusClient__ that creates a TCP connection with localhost:7000,
        //  with a thread pool of 4 workers.
        EventBusClient::new(std::net::SocketAddr::from(([127, 0, 0, 1], 7000)), 4)
    {
        // Registering an **address** called "welcome" and giving the **handler** function,
        // which basically prints the message to stdout.
        eb_client
            .register(
                "welcome".to_owned(),
                Arc::new(|msg: Value| {
                    println!("{msg:?}");
                }),
            )?
            // Client doesn't start to process the messages until __start_listening__ is called.
            .start_listening();

        // After 10 seconds clint gets stopped.
        std::thread::sleep(std::time::Duration::from_secs(3));
        eb_client.stop_listening();

        std::thread::sleep(std::time::Duration::from_secs(120));
    };
    Ok(())
}
