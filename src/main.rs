use clap::Parser;
use corestream::messages::cluster_message::Payload;
use corestream::messages::{ClusterMessage, ClusterPing, ProducerPayload, ServerAck};
use corestream::storage::StorageEngine;
use prost::Message;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

const MSG_TYPE_PRODUCER: u8 = 0;
const MSG_TYPE_CLUSTER: u8 = 1;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    node_id: u32,

    #[arg(short, long, default_value_t = 9092)]
    port: u16,

    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = format!("127.0.0.1:{}", args.port);
    
    // Each node gets its own isolated disk directory
    let data_dir = format!("data_node_{}", args.node_id);
    let storage = Arc::new(Mutex::new(StorageEngine::new(&data_dir)?));

    println!("CoreStream Node {} starting on {}", args.node_id, addr);
    if !args.peers.is_empty() {
        println!("Configured peers: {:?}", args.peers);
    }

    // --- Cluster Discovery (Peer-to-Peer) Task ---
    let peers = args.peers.clone();
    let node_id = args.node_id;
    tokio::spawn(async move {
        loop {
            for peer_addr in &peers {
                if let Ok(mut stream) = TcpStream::connect(peer_addr).await {
                    let ping = ClusterMessage {
                        sender_node_id: node_id,
                        payload: Some(Payload::Ping(ClusterPing { is_leader: false })),
                    };
                    
                    let mut buf = Vec::new();
                    ping.encode(&mut buf).unwrap();
                    
                    // Framing: [4-Byte Length] [1-Byte Type] [Protobuf Payload]
                    let len = (buf.len() as u32 + 1).to_be_bytes(); // +1 for the type byte
                    let _ = stream.write_all(&len).await;
                    let _ = stream.write_all(&[MSG_TYPE_CLUSTER]).await;
                    let _ = stream.write_all(&buf).await;
                }
            }
            sleep(Duration::from_secs(3)).await; // Ping peers every 3 seconds
        }
    });

    // --- TCP Server Task ---
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let storage_clone = storage.clone();
        
        tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];
                if socket.read_exact(&mut len_buf).await.is_err() { return; }
                
                let total_len = u32::from_be_bytes(len_buf) as usize;
                if total_len == 0 { continue; }
                
                // Read the 1-byte message type discriminator
                let mut type_buf = [0u8; 1];
                if socket.read_exact(&mut type_buf).await.is_err() { return; }
                let msg_type = type_buf[0];
                
                // Read the actual protobuf payload
                let payload_len = total_len - 1;
                let mut payload_buf = vec![0u8; payload_len];
                if socket.read_exact(&mut payload_buf).await.is_err() { return; }
                
                // Multiplex based on the message type
                if msg_type == MSG_TYPE_CLUSTER {
                    if let Ok(cluster_msg) = ClusterMessage::decode(&payload_buf[..]) {
                        if let Some(Payload::Ping(ping)) = cluster_msg.payload {
                            println!("[Cluster] Heartbeat from Node {} | is_leader: {}", 
                                     cluster_msg.sender_node_id, ping.is_leader);
                        }
                    }
                } else if msg_type == MSG_TYPE_PRODUCER {
                    if let Ok(payload) = ProducerPayload::decode(&payload_buf[..]) {
                        println!("[Client] Received Payload | Topic: '{}' | Data: {} bytes", 
                                 payload.topic, payload.data.len());
                        
                        let offset = {
                            let mut engine = storage_clone.lock().await;
                            engine.append(&payload_buf).unwrap_or(0)
                        };
                        
                        let ack = ServerAck {
                            success: true,
                            error_message: String::new(),
                            offset, 
                        };
                        
                        let mut ack_buf = Vec::new();
                        ack.encode(&mut ack_buf).unwrap();
                        
                        let ack_len = (ack_buf.len() as u32).to_be_bytes();
                        let _ = socket.write_all(&ack_len).await;
                        let _ = socket.write_all(&ack_buf).await;
                    }
                }
            }
        });
    }
}
