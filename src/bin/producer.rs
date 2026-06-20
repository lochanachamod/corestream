use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};
use corestream::messages::{ProducerPayload, ServerAck};

const MSG_TYPE_PRODUCER: u8 = 0;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9092";
    println!("Producer connecting to CoreStream at {}", addr);
    
    let mut socket = TcpStream::connect(addr).await?;
    
    // -- AUTHENTICATE --
    let auth = corestream::messages::AuthHandshake {
        api_key: std::env::var("CORESTREAM_API_KEY").unwrap_or_else(|_| "super_secret_corestream_key".to_string()),
    };
    let mut auth_buf = Vec::new();
    auth.encode(&mut auth_buf).unwrap();
    let auth_len = (auth_buf.len() as u32 + 1).to_be_bytes();
    
    socket.write_all(&auth_len).await?;
    socket.write_all(&[4]).await?; // MSG_TYPE_AUTH
    socket.write_all(&auth_buf).await?;
    
    let mut ack = [0u8; 1];
    socket.read_exact(&mut ack).await?;
    if ack[0] != 1 {
        println!("❌ Security Exception: Authentication failed! The Server rejected the API Key.");
        return Ok(());
    }

    println!("✅ Authenticated successfully! Blasting 5 payloads...");

    for i in 1..=5 {
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

        // Send length (including the 1-byte type header)
        let payload_len = (payload_buf.len() as u32 + 1).to_be_bytes();
        socket.write_all(&payload_len).await?;
        
        // Send the message type discriminator
        socket.write_all(&[MSG_TYPE_PRODUCER]).await?;
        
        // Send the payload
        socket.write_all(&payload_buf).await?;
        
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
