# Contributing to CoreStream

First off, thank you for considering contributing to CoreStream! It's people like you that make this engine such a great alternative to heavy JVM-based brokers.

## How Can I Contribute?

### 1. Reporting Bugs
If you find a bug in the source code or a mistake in the documentation, you can help us by submitting an issue to our GitHub Repository.

### 2. Suggesting Enhancements
If you have ideas to make CoreStream better, faster, or more secure, please submit an issue detailing your idea.

### 3. Pull Requests
CoreStream is an open-source project, and we welcome code contributions!
* **Fork the repository** and create your branch from `main`.
* **Run tests** to ensure your changes do not break the Raft consensus algorithms or Zero-Copy memory mappings.
* **Keep your commits small** and focused on a single logical change.
* **Write clear commit messages**.

## Future Development Roadmap
We are actively looking for contributors to help with the following future features:
- **Rustls Integration:** Upgrading the raw TCP sockets to enforce full TLS encryption in transit.
- **Consumer Groups:** Implementing stateful Consumer Group tracking (similar to Kafka) so consumers can resume reading where they left off.
- **Go and Java SDKs:** Expanding our Client SDK ecosystem.

## Development Setup
1. Clone the repo: `git clone https://github.com/lochanachamod/corestream.git`
2. Run the cluster locally: `cargo run --bin corestream -- --node-id 1 --port 9092 --peers 127.0.0.1:9093,127.0.0.1:9094`
3. Contribute your changes!
