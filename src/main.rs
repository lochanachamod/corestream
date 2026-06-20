use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::sync::Arc;
use tokio::sync::Mutex;
use corestream::messages::{ProducerPayload, ServerAck};
use corestream::storage::StorageEngine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9092";
    let listener = TcpListener::bind(addr).await?;
    
    // Initialize the Storage Engine
    let storage = Arc::new(Mutex::new(StorageEngine::new("data")?));
    
    println!("CoreStream Leader listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;
        let storage_clone = storage.clone();
        
        tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];
                
                if let Err(_) = socket.read_exact(&mut len_buf).await {
                    return; 
                }
                
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut payload_buf = vec![0u8; msg_len];
                
                if let Err(_) = socket.read_exact(&mut payload_buf).await {
                    return;
                }
                
                match ProducerPayload::decode(&payload_buf[..]) {
                    Ok(payload) => {
                        println!("Received Payload | Topic: '{}' | Data: {} bytes | Timestamp: {}", 
                                 payload.topic, payload.data.len(), payload.timestamp);
                        
                        // Append to our disk-backed Storage Engine
                        let offset = {
                            let mut engine = storage_clone.lock().await;
                            // Re-encode the decoded payload to save the raw binary directly
                            // Alternatively, we could just save `payload_buf` directly!
                            // Since `payload_buf` is the exact binary Protobuf, let's write it to disk.
                            engine.append(&payload_buf).expect("Failed to write to disk")
                        };
                        
                        let ack = ServerAck {
                            success: true,
                            error_message: String::new(),
                            offset, // Now returning the REAL offset from our commit log!
                        };
                        
                        let mut ack_buf = Vec::new();
                        ack.encode(&mut ack_buf).unwrap();
                        
                        let ack_len = (ack_buf.len() as u32).to_be_bytes();
                        let _ = socket.write_all(&ack_len).await;
                        let _ = socket.write_all(&ack_buf).await;
                    }
                    Err(e) => {
                        eprintln!("Failed to decode payload: {}", e);
                    }
                }
            }
        });
    }
}
