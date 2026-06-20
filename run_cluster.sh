#!/bin/bash
echo "Cleaning up any running instances..."
pkill -f corestream

echo "Building project..."
cargo build

echo "Starting Node 1 (Port 9092)..."
cargo run --bin corestream -- --node-id 1 --port 9092 --peers 127.0.0.1:9093,127.0.0.1:9094 &
PID1=$!

echo "Starting Node 2 (Port 9093)..."
cargo run --bin corestream -- --node-id 2 --port 9093 --peers 127.0.0.1:9092,127.0.0.1:9094 &
PID2=$!

echo "Starting Node 3 (Port 9094)..."
cargo run --bin corestream -- --node-id 3 --port 9094 --peers 127.0.0.1:9092,127.0.0.1:9093 &
PID3=$!

echo "Cluster is running across 3 independent nodes!"
echo "Press Ctrl+C to kill the cluster."

# Trap Ctrl+C to kill all background jobs cleanly
trap "echo 'Shutting down cluster...'; kill $PID1 $PID2 $PID3; exit" INT

wait
