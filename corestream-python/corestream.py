import socket
import struct
import time
import messages_pb2

class CoreStreamClient:
    def __init__(self, host: str, port: int, api_key: str):
        self.host = host
        self.port = port
        self.api_key = api_key
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()

    def connect(self):
        self.socket.connect((self.host, self.port))
        
        # 1. Send AuthHandshake
        auth = messages_pb2.AuthHandshake(api_key=self.api_key)
        auth_bytes = auth.SerializeToString()
        
        # Length is payload length + 1 byte for MSG_TYPE
        length = len(auth_bytes) + 1
        self.socket.sendall(struct.pack(">I", length))
        
        # MSG_TYPE_AUTH = 4
        self.socket.sendall(bytes([4]))
        self.socket.sendall(auth_bytes)
        
        # Await ACK
        ack = self.socket.recv(1)
        if not ack or ack[0] != 1:
            raise PermissionError("CoreStream Authentication Failed. Invalid API Key.")
            
        print(f"✅ Connected and Authenticated to CoreStream at {self.host}:{self.port}")

    def produce(self, topic: str, data: bytes):
        payload = messages_pb2.ProducerPayload(
            topic=topic,
            data=data,
            timestamp=int(time.time())
        )
        payload_bytes = payload.SerializeToString()
        
        length = len(payload_bytes) + 1
        self.socket.sendall(struct.pack(">I", length))
        
        # MSG_TYPE_PRODUCER = 0
        self.socket.sendall(bytes([0]))
        self.socket.sendall(payload_bytes)

# Example Usage
if __name__ == "__main__":
    client = CoreStreamClient("127.0.0.1", 9092, "super_secret_corestream_key")
    client.produce("python_logs", b"Hello from the official Python SDK!")
    print("🚀 Successfully published message via Python SDK!")
