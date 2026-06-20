use clap::Parser;
use corestream::messages::cluster_message::Payload;
use corestream::messages::{
    AppendEntries, AppendEntriesResponse, ClusterMessage, ConsumerRequest, ConsumerResponse, ProducerPayload, RequestVote, ServerAck, VoteResponse,
};
use corestream::storage::StorageEngine;
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex};
use tokio::time::sleep;

const MSG_TYPE_PRODUCER: u8 = 0;
const MSG_TYPE_CLUSTER: u8 = 1;
const MSG_TYPE_CONSUMER: u8 = 2;

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
    
    // Log Replication State
    match_index: HashMap<u32, u64>,
    commit_index: u64,
    pending_acks: HashMap<u64, oneshot::Sender<u64>>,
}

impl RaftState {
    fn new() -> Self {
        Self {
            current_term: 0,
            role: NodeRole::Follower,
            voted_for: None,
            votes_received: HashSet::new(),
            last_heartbeat: Instant::now(),
            match_index: HashMap::new(),
            commit_index: 0,
            pending_acks: HashMap::new(),
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
            let election_timeout = (rand::random::<u128>() % 500) + 500;
            sleep(Duration::from_millis(50)).await;

            let mut state = raft_clone.lock().await;
            
            if state.role == NodeRole::Leader {
                // Assert authority with heartbeats every 50ms
                let msg = ClusterMessage {
                    sender_node_id: node_id,
                    payload: Some(Payload::AppendEntries(AppendEntries {
                        term: state.current_term,
                        leader_commit: state.commit_index,
                        entries: vec![], // Empty heartbeat
                    })),
                };
                broadcast_cluster_message(&peers, msg).await;
            } else {
                if state.last_heartbeat.elapsed().as_millis() >= election_timeout {
                    println!("\n[Node {}] ELECTION TIMEOUT! Becoming Candidate for Term {}", node_id, state.current_term + 1);
                    state.role = NodeRole::Candidate;
                    state.current_term += 1;
                    state.voted_for = Some(node_id);
                    state.votes_received.clear();
                    state.votes_received.insert(node_id);
                    state.last_heartbeat = Instant::now();

                    let majority = (peers.len() + 1) / 2 + 1;
                    if state.votes_received.len() >= majority {
                        println!("==> [Node {}] WON ELECTION! Becoming LEADER for Term {} <==", node_id, state.current_term);
                        state.role = NodeRole::Leader;
                        
                        // Setup match tracking
                        state.match_index.clear();
                        for p in &peers {
                            if let Some(port_str) = p.split(':').last() {
                                if let Ok(port) = port_str.parse::<u32>() {
                                    state.match_index.insert(port - 9091, 0); // E.g., Port 9092 = Node 1
                                }
                            }
                        }
                    } else {
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
        let node_id = args.node_id;
        
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
                        let sender_id = cluster_msg.sender_node_id;
                        
                        match cluster_msg.payload {
                            Some(Payload::AppendEntries(ae)) => {
                                if ae.term >= state.current_term {
                                    if state.role != NodeRole::Follower {
                                        println!("[Node {}] Stepping down. Recognized Node {} as Leader.", node_id, sender_id);
                                    }
                                    state.current_term = ae.term;
                                    state.role = NodeRole::Follower;
                                    state.last_heartbeat = Instant::now();
                                    
                                    let mut highest_offset = 0;
                                    if !ae.entries.is_empty() {
                                        let mut engine = storage_clone.lock().await;
                                        for entry in ae.entries {
                                            highest_offset = engine.append(&entry).unwrap_or(highest_offset);
                                        }
                                        println!("[Node {}] REPLICATED {} logs from Leader {} to disk!", node_id, highest_offset, sender_id);
                                        
                                        let target_peer = peers_clone.iter().find(|p| p.contains(&format!(":{}", 9091 + sender_id)));
                                        if let Some(peer_addr) = target_peer {
                                            let resp = ClusterMessage {
                                                sender_node_id: node_id,
                                                payload: Some(Payload::AppendEntriesResponse(AppendEntriesResponse {
                                                    term: state.current_term,
                                                    success: true,
                                                    match_index: highest_offset,
                                                })),
                                            };
                                            broadcast_cluster_message(&[peer_addr.clone()], resp).await;
                                        }
                                    }
                                }
                            }
                            Some(Payload::AppendEntriesResponse(aer)) => {
                                if state.role == NodeRole::Leader && aer.success {
                                    state.match_index.insert(sender_id, aer.match_index);
                                    
                                    // Check majority consensus
                                    let majority = (peers_clone.len() + 1) / 2 + 1;
                                    let mut count = 1; // Leader implicitly has it
                                    for (&_peer, &idx) in state.match_index.iter() {
                                        if idx >= aer.match_index { count += 1; }
                                    }
                                    
                                    if count >= majority && aer.match_index > state.commit_index {
                                        println!(">>> [Leader {}] CONSENSUS REACHED! Log Index {} committed across majority!", node_id, aer.match_index);
                                        state.commit_index = aer.match_index;
                                        if let Some(tx) = state.pending_acks.remove(&aer.match_index) {
                                            let _ = tx.send(aer.match_index); // Unblock the Producer!
                                        }
                                    }
                                }
                            }
                            Some(Payload::RequestVote(rv)) => {
                                let mut vote_granted = false;
                                if rv.term > state.current_term {
                                    state.current_term = rv.term;
                                    state.role = NodeRole::Follower;
                                    state.voted_for = None;
                                }
                                if rv.term == state.current_term && (state.voted_for.is_none() || state.voted_for == Some(sender_id)) {
                                    vote_granted = true;
                                    state.voted_for = Some(sender_id);
                                    state.last_heartbeat = Instant::now();
                                }
                                let target_peer = peers_clone.iter().find(|p| p.contains(&format!(":{}", 9091 + sender_id)));
                                if let Some(peer_addr) = target_peer {
                                    let resp = ClusterMessage {
                                        sender_node_id: node_id,
                                        payload: Some(Payload::VoteResponse(VoteResponse { term: state.current_term, vote_granted })),
                                    };
                                    broadcast_cluster_message(&[peer_addr.clone()], resp).await;
                                }
                            }
                            Some(Payload::VoteResponse(vr)) => {
                                if state.role == NodeRole::Candidate && vr.term == state.current_term && vr.vote_granted {
                                    state.votes_received.insert(sender_id);
                                    let majority = (peers_clone.len() + 1) / 2 + 1;
                                    if state.votes_received.len() >= majority {
                                        println!("\n==> [Node {}] WON ELECTION! Becoming LEADER for Term {} <==", node_id, state.current_term);
                                        state.role = NodeRole::Leader;
                                    }
                                } else if vr.term > state.current_term {
                                    state.current_term = vr.term;
                                    state.role = NodeRole::Follower;
                                }
                            }
                            _ => {}
                        }
                    }
                } else if msg_type == MSG_TYPE_PRODUCER {
                    let mut is_leader = false;
                    let mut term = 0;
                    {
                        let state = raft_clone.lock().await;
                        is_leader = state.role == NodeRole::Leader;
                        term = state.current_term;
                    }
                    
                    if !is_leader {
                        println!("[Node {}] Rejecting producer write, I am not the leader.", node_id);
                        continue;
                    }

                    // 1. Leader writes to local storage
                    let offset = {
                        let mut engine = storage_clone.lock().await;
                        engine.append(&payload_buf).unwrap_or(0)
                    };
                    println!("[Leader {}] Saved payload to local disk (offset {}). Waiting for replication...", node_id, offset);
                    
                    // 2. Setup channel to wait for majority replication
                    let (tx, rx) = oneshot::channel();
                    {
                        let mut state = raft_clone.lock().await;
                        state.pending_acks.insert(offset, tx);
                    }
                    
                    // 3. Replicate to followers
                    let msg = ClusterMessage {
                        sender_node_id: node_id,
                        payload: Some(Payload::AppendEntries(AppendEntries {
                            term,
                            leader_commit: 0,
                            entries: vec![payload_buf.clone()],
                        })),
                    };
                    broadcast_cluster_message(&peers_clone, msg).await;
                    
                    // 4. Block until majority replication is confirmed!
                    if let Ok(committed_offset) = rx.await {
                        let ack = ServerAck { success: true, error_message: String::new(), offset: committed_offset };
                        let mut ack_buf = Vec::new();
                        ack.encode(&mut ack_buf).unwrap();
                        let ack_len = (ack_buf.len() as u32).to_be_bytes();
                        let _ = socket.write_all(&ack_len).await;
                        let _ = socket.write_all(&ack_buf).await;
                    }
                } else if msg_type == MSG_TYPE_CONSUMER {
                    if let Ok(req) = ConsumerRequest::decode(&payload_buf[..]) {
                        // Attempt to read the requested offset directly from the OS Page Cache
                        let result = {
                            let engine = storage_clone.lock().await;
                            engine.read(req.offset)
                        };

                        let resp = match result {
                            Ok(Some(bytes)) => ConsumerResponse {
                                success: true,
                                error_message: String::new(),
                                payload_bytes: bytes,
                            },
                            Ok(None) => ConsumerResponse {
                                success: false,
                                error_message: String::from("Offset not found"),
                                payload_bytes: vec![],
                            },
                            Err(e) => ConsumerResponse {
                                success: false,
                                error_message: e.to_string(),
                                payload_bytes: vec![],
                            },
                        };

                        let mut resp_buf = Vec::new();
                        resp.encode(&mut resp_buf).unwrap();
                        let resp_len = (resp_buf.len() as u32).to_be_bytes();
                        let _ = socket.write_all(&resp_len).await;
                        let _ = socket.write_all(&resp_buf).await;
                    }
                }
            }
        });
    }
}
