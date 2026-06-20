# 🔌 CoreStream Developer Integration Guide

Welcome to the CoreStream Integration Guide! If you are a developer or an enterprise architect looking to integrate CoreStream into your own company's microservices, web applications, or data pipelines, this guide will show you exactly how to do it.

---

## 1. The Core Concept
Unlike traditional databases where you query data (SQL), CoreStream is an **Event Streaming Broker**. 
Your applications do not "query" CoreStream. Instead:
1. **Producers** (your web apps, microservices, IoT devices) push events to a specific "Topic" in CoreStream.
2. **CoreStream** safely stores and replicates that event across its cluster.
3. **Consumers** (your analytics engines, shipping services, notification workers) pull the events out of CoreStream at their own pace.

---

## 2. Booting the Engine
Before your apps can connect, you need the engine running. On your company's servers (or locally for testing), clone this repository and boot the cluster:

```bash
git clone https://github.com/lochanachamod/corestream.git
cd corestream

# Terminal 1
cargo run --bin corestream -- --node-id 1 --port 9092 --peers 127.0.0.1:9093,127.0.0.1:9094
# Terminal 2
cargo run --bin corestream -- --node-id 2 --port 9093 --peers 127.0.0.1:9092,127.0.0.1:9094
# Terminal 3
cargo run --bin corestream -- --node-id 3 --port 9094 --peers 127.0.0.1:9092,127.0.0.1:9093
```
*The cluster is now alive and listening on `127.0.0.1:9092`.*

---

## 3. Integrating with a Node.js Web Application
Imagine you are building a Next.js or Express web app, and you want to track every time a user clicks the "Checkout" button.

**Step 1: Copy the SDK**
Copy the `corestream-node` folder from this repository into your own web app's codebase. Ensure you also copy the `proto/messages.proto` file so the SDK knows how to serialize the data.

**Step 2: Connect and Publish**
Inside your web app's API route or Express controller, import the client and fire an event:

```javascript
const CoreStreamClient = require('./corestream-node/corestream');

async function handleCheckout(req, res) {
    const user = req.body.user;
    const cart = req.body.cart;

    try {
        // 1. Connect to the CoreStream Leader
        const client = new CoreStreamClient('127.0.0.1', 9092, 'super_secret_corestream_key');
        await client.connect();

        // 2. Format your data as JSON
        const eventData = JSON.stringify({ action: "CHECKOUT", user: user, cart: cart });

        // 3. Publish to the "user_events" topic!
        await client.produce('user_events', eventData);

        res.status(200).send("Checkout processing in background!");
    } catch (err) {
        console.error("Failed to stream event:", err);
        res.status(500).send("Server Error");
    }
}
```
*Notice how fast this is. The web app doesn't wait for the payment to process; it just throws the event into CoreStream and immediately responds to the user!*

---

## 4. Integrating with a Python Data Science Service
Now that the Node.js app is pushing "Checkout" events into CoreStream, you might have a separate Python service that reads those events, processes the payments, or runs Machine Learning algorithms on them.

**Step 1: Copy the SDK**
Copy the `corestream-python` folder and `proto/messages.proto` into your Python microservice. 

**Step 2: Connect and Publish/Consume**
```python
from corestream import CoreStreamClient
import json

# 1. Connect to the CoreStream Leader
client = CoreStreamClient("127.0.0.1", 9092, "super_secret_corestream_key")

# 2. You can publish data just like Node.js
payment_result = json.dumps({"status": "SUCCESS", "user": "John"})
client.produce("payment_results", payment_result.encode('utf-8'))

print("Payment Result Streamed to CoreStream!")
```

---

## 5. Security & Network Considerations
When deploying to a production environment (like AWS or Google Cloud):
1. **API Keys:** Change the default `super_secret_corestream_key` by setting the `CORESTREAM_API_KEY` environment variable on all servers. The Zero-Trust firewall will drop any SDK connection that doesn't use the correct key.
2. **Ports:** Ensure your firewall allows TCP traffic on ports `9092`, `9093`, and `9094`.
3. **Data Retention:** You do not need to manually delete old data. The background Garbage Collector thread is hardcoded to automatically sweep segments.

---
*Built for High-Performance Distributed Computing.*
