use std::sync::{mpsc, Arc, Mutex};
use std::thread;

pub type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct Worker {
    pub id: usize,
    pub thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();

            match message {
                Ok(job) => {
                    println!("Worker {id} got a message; processing...");
                    job();
                }
                Err(_) => {
                    eprintln!("Worker {id} disconnected; shutting down...");
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}
