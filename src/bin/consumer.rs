use corestream::messages::{ConsumerRequest, ConsumerResponse, ProducerPayload};
use prost::Message;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MSG_TYPE_CONSUMER: u8 = 2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9092";
    println!("Consumer connecting to CoreStream at {}", addr);
    
    let mut stream = TcpStream::connect(addr).await?;
    println!("Connected successfully! Beginning Zero-Copy read of logs...\n");

    let start = Instant::now();
    let mut offsets_fetched = 0;
    
    for offset in 1..=5 {
        let req = ConsumerRequest {
            topic: "trade_logs".to_string(),
            offset,
        };

        let mut req_buf = Vec::new();
        req.encode(&mut req_buf)?;
        
        let req_len = (req_buf.len() as u32 + 1).to_be_bytes();
        
        // 1. Send Length
        stream.write_all(&req_len).await?;
        // 2. Send Type Byte
        stream.write_all(&[MSG_TYPE_CONSUMER]).await?;
        // 3. Send Protobuf binary
        stream.write_all(&req_buf).await?;

        // 4. Read Response Length
        let mut len_buf = [0u8; 4];
        if let Err(e) = stream.read_exact(&mut len_buf).await {
            println!("Failed to read response length: {}", e);
            break;
        }
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        // 5. Read Response Protobuf
        let mut resp_buf = vec![0u8; resp_len];
        if let Err(e) = stream.read_exact(&mut resp_buf).await {
            println!("Failed to read response: {}", e);
            break;
        }

        let resp = ConsumerResponse::decode(&resp_buf[..])?;
        if resp.success {
            // The zero-copy payload bytes sent straight from the server disk!
            let payload = ProducerPayload::decode(&resp.payload_bytes[..])?;
            let data_str = String::from_utf8_lossy(&payload.data);
            println!("[Offset {}] SUCCESS | Topic: {} | Data: '{}'", offset, payload.topic, data_str);
            offsets_fetched += 1;
        } else {
            println!("[Offset {}] FETCH FAILED: {}", offset, resp.error_message);
        }
    }

    println!("\nFetched {} messages in {:?}", offsets_fetched, start.elapsed());

    Ok(())
}
