use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use corestream::messages::{ProducerPayload, ServerAck};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9092";
    let listener = TcpListener::bind(addr).await?;
    println!("CoreStream Leader listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;
        
        tokio::spawn(async move {
            // Keep the connection open and process a stream of messages
            loop {
                let mut len_buf = [0u8; 4];
                
                // 1. Frame Decoding: Read the length of the incoming message
                if let Err(_) = socket.read_exact(&mut len_buf).await {
                    return; // Socket closed or read error, disconnect client
                }
                
                let msg_len = u32::from_be_bytes(len_buf) as usize;
                let mut payload_buf = vec![0u8; msg_len];
                
                // 2. Read the exact Protobuf binary payload
                if let Err(_) = socket.read_exact(&mut payload_buf).await {
                    return;
                }
                
                // 3. Deserialize using Prost
                match ProducerPayload::decode(&payload_buf[..]) {
                    Ok(payload) => {
                        println!("Received Payload | Topic: '{}' | Data: {} bytes | Timestamp: {}", 
                                 payload.topic, payload.data.len(), payload.timestamp);
                        
                        // 4. Send Acknowledgment back
                        let ack = ServerAck {
                            success: true,
                            error_message: String::new(),
                            offset: 1, 
                        };
                        
                        let mut ack_buf = Vec::new();
                        ack.encode(&mut ack_buf).unwrap();
                        
                        // Frame the ACK
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
