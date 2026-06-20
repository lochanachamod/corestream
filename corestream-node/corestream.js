const net = require('net');
const protobuf = require('protobufjs');

class CoreStreamClient {
    constructor(host, port, apiKey) {
        this.host = host;
        this.port = port;
        this.apiKey = apiKey;
        this.client = new net.Socket();
        this.root = null;
    }

    async connect() {
        // Load the Protobuf schema dynamically
        this.root = await protobuf.load('../proto/messages.proto');
        
        return new Promise((resolve, reject) => {
            this.client.connect(this.port, this.host, () => {
                const AuthHandshake = this.root.lookupType('AuthHandshake');
                const errMsg = AuthHandshake.verify({ api_key: this.apiKey });
                if (errMsg) throw Error(errMsg);

                const message = AuthHandshake.create({ api_key: this.apiKey });
                const buffer = AuthHandshake.encode(message).finish();

                const lengthBuf = Buffer.alloc(4);
                lengthBuf.writeUInt32BE(buffer.length + 1, 0);

                this.client.write(lengthBuf);
                this.client.write(Buffer.from([4])); // MSG_TYPE_AUTH
                this.client.write(buffer);
            });

            this.client.once('data', (data) => {
                if (data[0] === 1) {
                    console.log(`✅ Connected and Authenticated to CoreStream at ${this.host}:${this.port}`);
                    resolve();
                } else {
                    reject(new Error("CoreStream Authentication Failed. Invalid API Key."));
                }
            });

            this.client.on('error', reject);
        });
    }

    async produce(topic, dataString) {
        const ProducerPayload = this.root.lookupType('ProducerPayload');
        
        const payload = {
            topic: topic,
            data: Buffer.from(dataString),
            timestamp: Math.floor(Date.now() / 1000)
        };

        const errMsg = ProducerPayload.verify(payload);
        if (errMsg) throw Error(errMsg);

        const message = ProducerPayload.create(payload);
        const buffer = ProducerPayload.encode(message).finish();

        const lengthBuf = Buffer.alloc(4);
        lengthBuf.writeUInt32BE(buffer.length + 1, 0);

        this.client.write(lengthBuf);
        this.client.write(Buffer.from([0])); // MSG_TYPE_PRODUCER
        this.client.write(buffer);
    }
}

// Example Usage
(async () => {
    try {
        const client = new CoreStreamClient('127.0.0.1', 9092, 'super_secret_corestream_key');
        await client.connect();
        await client.produce('node_logs', 'Hello from the official Node.js SDK!');
        console.log('🚀 Successfully published message via Node SDK!');
        process.exit(0);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
})();
