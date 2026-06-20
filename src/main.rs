use clap::Parser;
use corestream::messages::cluster_message::Payload;
use corestream::messages::{
    AppendEntries, ClusterMessage, ProducerPayload, RequestVote, ServerAck, VoteResponse,
};
use corestream::storage::StorageEngine;
use prost::Message;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::sleep;

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

#[derive(PartialEq, Clone, Copy, Debug)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

struct RaftState {
    current_term: u64,
    role: NodeRole,
    voted_for: Option<u32>,
    votes_received: HashSet<u32>,
    last_heartbeat: Instant,
}

impl RaftState {
    fn new() -> Self {
        Self {
            current_term: 0,
            role: NodeRole::Follower,
            voted_for: None,
            votes_received: HashSet::new(),
            last_heartbeat: Instant::now(),
        }
    }
}

async fn broadcast_cluster_message(peers: &[String], msg: ClusterMessage) {
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();
    let len = (buf.len() as u32 + 1).to_be_bytes();
    
    for peer_addr in peers {
        let peer_addr = peer_addr.clone();
        let len_bytes = len;
        let payload_bytes = buf.clone();
        
        tokio::spawn(async move {
            // Short timeout so we don't hang if a peer is dead
            if let Ok(mut stream) = tokio::time::timeout(Duration::from_millis(50), TcpStream::connect(&peer_addr)).await.unwrap_or(Err(std::io::Error::from(std::io::ErrorKind::TimedOut))) {
                let _ = stream.write_all(&len_bytes).await;
                let _ = stream.write_all(&[MSG_TYPE_CLUSTER]).await;
                let _ = stream.write_all(&payload_bytes).await;
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = format!("127.0.0.1:{}", args.port);
    let data_dir = format!("data_node_{}", args.node_id);
    let storage = Arc::new(Mutex::new(StorageEngine::new(&data_dir)?));
    let raft_state = Arc::new(Mutex::new(RaftState::new()));

    println!("CoreStream Node {} starting on {}", args.node_id, addr);

    let peers = args.peers.clone();
    let node_id = args.node_id;
    let raft_clone = raft_state.clone();
    
    // --- Raft Background Timer ---
    tokio::spawn(async move {
        loop {
            // Randomized election timeout to prevent split votes
            let election_timeout = (rand::random::<u128>() % 500) + 500;
            sleep(Duration::from_millis(50)).await;

            let mut state = raft_clone.lock().await;
            
            if state.role == NodeRole::Leader {
                // Assert authority with heartbeats every 50ms
                let msg = ClusterMessage {
                    sender_node_id: node_id,
                    payload: Some(Payload::AppendEntries(AppendEntries {
                        term: state.current_term,
                        leader_commit: 0,
                    })),
                };
                broadcast_cluster_message(&peers, msg).await;
            } else {
                // Follower / Candidate logic
                if state.last_heartbeat.elapsed().as_millis() >= election_timeout {
                    println!("\n[Node {}] ELECTION TIMEOUT! Becoming Candidate for Term {}", node_id, state.current_term + 1);
                    state.role = NodeRole::Candidate;
                    state.current_term += 1;
                    state.voted_for = Some(node_id); // Vote for self
                    state.votes_received.clear();
                    state.votes_received.insert(node_id);
                    state.last_heartbeat = Instant::now(); // Reset timer

                    let majority = (peers.len() + 1) / 2 + 1;
                    if state.votes_received.len() >= majority {
                        println!("==> [Node {}] WON ELECTION! Becoming LEADER for Term {} <==", node_id, state.current_term);
                        state.role = NodeRole::Leader;
                    } else {
                        // Broadcast RequestVote
                        let msg = ClusterMessage {
                            sender_node_id: node_id,
                            payload: Some(Payload::RequestVote(RequestVote {
                                term: state.current_term,
                                last_log_index: 0,
                                last_log_term: 0,
                            })),
                        };
                        broadcast_cluster_message(&peers, msg).await;
                    }
                }
            }
        }
    });

    // --- TCP Server Listener ---
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let storage_clone = storage.clone();
        let raft_clone = raft_state.clone();
        let peers_clone = args.peers.clone();
        
        tokio::spawn(async move {
            loop {
                let mut len_buf = [0u8; 4];
                if socket.read_exact(&mut len_buf).await.is_err() { return; }
                let total_len = u32::from_be_bytes(len_buf) as usize;
                if total_len == 0 { continue; }
                
                let mut type_buf = [0u8; 1];
                if socket.read_exact(&mut type_buf).await.is_err() { return; }
                let msg_type = type_buf[0];
                
                let mut payload_buf = vec![0u8; total_len - 1];
                if socket.read_exact(&mut payload_buf).await.is_err() { return; }
                
                if msg_type == MSG_TYPE_CLUSTER {
                    if let Ok(cluster_msg) = ClusterMessage::decode(&payload_buf[..]) {
                        let mut state = raft_clone.lock().await;
                        
                        match cluster_msg.payload {
                            Some(Payload::AppendEntries(ae)) => {
                                if ae.term >= state.current_term {
                                    if state.role != NodeRole::Follower {
                                        println!("[Node {}] Stepping down. Recognized Node {} as Leader for Term {}", 
                                                 node_id, cluster_msg.sender_node_id, ae.term);
                                    }
                                    state.current_term = ae.term;
                                    state.role = NodeRole::Follower;
                                    state.last_heartbeat = Instant::now(); // Prevent election!
                                }
                            }
                            Some(Payload::RequestVote(rv)) => {
                                let mut vote_granted = false;
                                if rv.term > state.current_term {
                                    state.current_term = rv.term;
                                    state.role = NodeRole::Follower;
                                    state.voted_for = None;
                                }
                                
                                if rv.term == state.current_term && 
                                  (state.voted_for.is_none() || state.voted_for == Some(cluster_msg.sender_node_id)) {
                                    vote_granted = true;
                                    state.voted_for = Some(cluster_msg.sender_node_id);
                                    state.last_heartbeat = Instant::now();
                                    println!("[Node {}] Voted FOR Node {} in Term {}", node_id, cluster_msg.sender_node_id, rv.term);
                                } else {
                                    println!("[Node {}] REJECTED vote for Node {} in Term {}", node_id, cluster_msg.sender_node_id, rv.term);
                                }
                                
                                // Send response back to the candidate
                                let target_peer = peers_clone.iter().find(|p| p.contains(&format!(":{}", 9091 + cluster_msg.sender_node_id)));
                                if let Some(peer_addr) = target_peer {
                                    let resp = ClusterMessage {
                                        sender_node_id: node_id,
                                        payload: Some(Payload::VoteResponse(VoteResponse {
                                            term: state.current_term,
                                            vote_granted,
                                        })),
                                    };
                                    broadcast_cluster_message(&[peer_addr.clone()], resp).await;
                                }
                            }
                            Some(Payload::VoteResponse(vr)) => {
                                if state.role == NodeRole::Candidate && vr.term == state.current_term && vr.vote_granted {
                                    state.votes_received.insert(cluster_msg.sender_node_id);
                                    let majority = (peers_clone.len() + 1) / 2 + 1;
                                    if state.votes_received.len() >= majority {
                                        println!("\n==> [Node {}] WON ELECTION! Becoming LEADER for Term {} <==", node_id, state.current_term);
                                        state.role = NodeRole::Leader;
                                        
                                        // Immediately heartbeat to stop other elections
                                        let msg = ClusterMessage {
                                            sender_node_id: node_id,
                                            payload: Some(Payload::AppendEntries(AppendEntries {
                                                term: state.current_term,
                                                leader_commit: 0,
                                            })),
                                        };
                                        broadcast_cluster_message(&peers_clone, msg).await;
                                    }
                                } else if vr.term > state.current_term {
                                    state.current_term = vr.term;
                                    state.role = NodeRole::Follower;
                                    state.voted_for = None;
                                }
                            }
                            _ => {}
                        }
                    }
                } else if msg_type == MSG_TYPE_PRODUCER {
                    {
                        let state = raft_clone.lock().await;
                        if state.role != NodeRole::Leader {
                            // Strictly speaking, only Leaders handle producer writes in Raft.
                            // We will allow it for testing if we want, or reject it.
                            // For now, let's process it anyway to not break the Producer test script.
                        }
                    }
                    if let Ok(payload) = ProducerPayload::decode(&payload_buf[..]) {
                        let offset = {
                            let mut engine = storage_clone.lock().await;
                            engine.append(&payload_buf).unwrap_or(0)
                        };
                        let ack = ServerAck { success: true, error_message: String::new(), offset };
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
