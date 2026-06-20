use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};
use corestream::messages::{ProducerPayload, ServerAck};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9092";
    println!("Producer connecting to CoreStream at {}", addr);
    
    let mut socket = TcpStream::connect(addr).await?;
    
    println!("Connected successfully! Blasting 5 payloads...");

    for i in 1..=5 {
        // Construct the Protobuf payload
        let payload = ProducerPayload {
            topic: String::from("trade_logs"),
            data: format!("Transaction Data Block #{}", i).into_bytes(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let mut payload_buf = Vec::new();
        payload.encode(&mut payload_buf).unwrap();

        // Send the 4-byte length prefix
        let payload_len = (payload_buf.len() as u32).to_be_bytes();
        socket.write_all(&payload_len).await?;
        
        // Send the serialized protobuf data
        socket.write_all(&payload_buf).await?;
        
        // Wait for the Server ACK
        let mut ack_len_buf = [0u8; 4];
        socket.read_exact(&mut ack_len_buf).await?;
        
        let ack_len = u32::from_be_bytes(ack_len_buf) as usize;
        let mut ack_buf = vec![0u8; ack_len];
        socket.read_exact(&mut ack_buf).await?;
        
        let ack = ServerAck::decode(&ack_buf[..])?;
        println!("Sent message #{} | Server ACK Success: {} | Offset: {}", i, ack.success, ack.offset);
    }

    Ok(())
}
