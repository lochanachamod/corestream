use corestream::messages::{TelemetryRequest, TelemetryResponse};
use prost::Message;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MSG_TYPE_TELEMETRY: u8 = 3;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::args().nth(1).unwrap_or_default();

    if mode == "--serve" {
        println!("🚀 Starting Telemetry HTTP Server on http://127.0.0.1:3000");
        let listener = TcpListener::bind("127.0.0.1:3000").await?;
        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0; 1024];
                    let _ = socket.read(&mut buf).await; // Read HTTP request
                    
                    let json_data = fetch_cluster_state().await;
                    
                    let response = format!(
                        "HTTP/1.1 200 OK\r\n\
                         Access-Control-Allow-Origin: *\r\n\
                         Content-Type: application/json\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\
                         \r\n\
                         {}",
                        json_data.len(),
                        json_data
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        }
    } else {
        // Normal CLI mode
        println!("========================================");
        println!("     CORESTREAM TELEMETRY DASHBOARD     ");
        println!("========================================\n");
        let _ = fetch_cluster_state().await;
    }
    Ok(())
}

async fn fetch_cluster_state() -> String {
    let nodes = vec![
        ("Node 1", "127.0.0.1:9092"),
        ("Node 2", "127.0.0.1:9093"),
        ("Node 3", "127.0.0.1:9094"),
    ];

    let mut json_results = Vec::new();
    let is_cli = std::env::args().nth(1).unwrap_or_default() != "--serve";

    for (name, addr) in nodes {
        let mut status = "OFFLINE".to_string();
        let mut role = "".to_string();
        let mut term = 0;
        let mut commit_index = 0;
        let mut storage_offset = 0;

        match tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
            Ok(Ok(mut stream)) => {
                let req = TelemetryRequest {};
                let mut req_buf = Vec::new();
                req.encode(&mut req_buf).unwrap();
                
                let req_len = (req_buf.len() as u32 + 1).to_be_bytes();
                if stream.write_all(&req_len).await.is_ok() && stream.write_all(&[MSG_TYPE_TELEMETRY]).await.is_ok() && stream.write_all(&req_buf).await.is_ok() {
                    let mut len_buf = [0u8; 4];
                    if stream.read_exact(&mut len_buf).await.is_ok() {
                        let resp_len = u32::from_be_bytes(len_buf) as usize;
                        let mut resp_buf = vec![0u8; resp_len];
                        if stream.read_exact(&mut resp_buf).await.is_ok() {
                            if let Ok(resp) = TelemetryResponse::decode(&resp_buf[..]) {
                                status = "ONLINE".to_string();
                                role = resp.role;
                                term = resp.current_term;
                                commit_index = resp.commit_index;
                                storage_offset = resp.storage_offset;
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        json_results.push(format!(
            r#"{{"name":"{}","address":"{}","status":"{}","role":"{}","term":{},"commit_index":{},"storage_offset":{}}}"#,
            name, addr, status, role, term, commit_index, storage_offset
        ));

        if is_cli {
            if status == "ONLINE" {
                let role_icon = if role == "Leader" { "👑" } else { "👥" };
                println!("{} [{}] - {}", role_icon, name, role.to_uppercase());
                println!("  ├─ Term:           {}", term);
                println!("  ├─ Commit Index:   {}", commit_index);
                println!("  └─ Storage Offset: {}\n", storage_offset);
            } else {
                println!("💀 [{}] - {}\n", name, status);
            }
        }
    }

    format!("[{}]", json_results.join(","))
}
