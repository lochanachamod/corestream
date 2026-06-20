use corestream::messages::{TelemetryRequest, TelemetryResponse};
use prost::Message;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MSG_TYPE_TELEMETRY: u8 = 3;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes = vec![
        ("Node 1", "127.0.0.1:9092"),
        ("Node 2", "127.0.0.1:9093"),
        ("Node 3", "127.0.0.1:9094"),
    ];

    println!("========================================");
    println!("     CORESTREAM TELEMETRY DASHBOARD     ");
    println!("========================================\n");

    for (name, addr) in nodes {
        match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
            Ok(Ok(mut stream)) => {
                let req = TelemetryRequest {};
                let mut req_buf = Vec::new();
                req.encode(&mut req_buf)?;
                
                let req_len = (req_buf.len() as u32 + 1).to_be_bytes();
                stream.write_all(&req_len).await?;
                stream.write_all(&[MSG_TYPE_TELEMETRY]).await?;
                stream.write_all(&req_buf).await?;

                let mut len_buf = [0u8; 4];
                if stream.read_exact(&mut len_buf).await.is_ok() {
                    let resp_len = u32::from_be_bytes(len_buf) as usize;
                    let mut resp_buf = vec![0u8; resp_len];
                    if stream.read_exact(&mut resp_buf).await.is_ok() {
                        if let Ok(resp) = TelemetryResponse::decode(&resp_buf[..]) {
                            let role_icon = if resp.role == "Leader" { "👑" } else { "👥" };
                            println!("{} [{}] - {}", role_icon, name, resp.role.to_uppercase());
                            println!("  ├─ Term:           {}", resp.current_term);
                            println!("  ├─ Commit Index:   {}", resp.commit_index);
                            println!("  └─ Storage Offset: {}\n", resp.storage_offset);
                            continue;
                        }
                    }
                }
                println!("❌ [{}] - BAD RESPONSE\n", name);
            }
            _ => {
                println!("💀 [{}] - OFFLINE\n", name);
            }
        }
    }

    Ok(())
}
