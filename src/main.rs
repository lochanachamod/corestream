mod messages;

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use messages::corestream::{ProducerPayload, ServerAck};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Port 9092 is standard for Kafka-like brokers
    let addr = "127.0.0.1:9092";
    let listener = TcpListener::bind(addr).await?;
    println!("CoreStream Leader listening on {}", addr);

    loop {
        // Accept incoming TCP connections concurrently
        let (mut socket, _) = listener.accept().await?;
        
        tokio::spawn(async move {
            let mut len_buf = [0u8; 4];
            
            // 1. Frame Decoding: Read the length of the incoming message (4 bytes, Big Endian)
            if let Err(_) = socket.read_exact(&mut len_buf).await {
                return; // Socket closed or read error
            }
            
            let msg_len = u32::from_be_bytes(len_buf) as usize;
            let mut payload_buf = vec![0u8; msg_len];
            
            // 2. Read the exact Protobuf binary payload from the stream
            if let Err(_) = socket.read_exact(&mut payload_buf).await {
                return;
            }
            
            // 3. Deserialize using Prost (Protobuf)
            match ProducerPayload::decode(&payload_buf[..]) {
                Ok(payload) => {
                    println!("Received Payload | Topic: '{}' | Data: {} bytes | Timestamp: {}", 
                             payload.topic, payload.data.len(), payload.timestamp);
                    
                    // 4. Send Acknowledgment back to Producer
                    let ack = ServerAck {
                        success: true,
                        error_message: String::new(),
                        offset: 1, // Dummy offset until Phase 2 (Storage Layer)
                    };
                    
                    let mut ack_buf = Vec::new();
                    ack.encode(&mut ack_buf).unwrap();
                    
                    // Frame the ACK with a 4-byte length header
                    let ack_len = (ack_buf.len() as u32).to_be_bytes();
                    let _ = socket.write_all(&ack_len).await;
                    let _ = socket.write_all(&ack_buf).await;
                }
                Err(e) => {
                    eprintln!("Failed to decode payload: {}", e);
                }
            }
        });
    }
}
