# Neural Network Broadcast Example

This demo shows how **memblast** can stream neural network weights to clients. The server trains a tiny PyTorch network to solve the XOR problem. Clients read the latest weights and use them for inference.

## Files

- `nn_server.py` – Trains the network and publishes the weights.
- `nn_client.py` – Connects to the server and prints XOR predictions using the newest weights.

## Usage

Start the server (default port 7040). The weights are written as raw bytes directly into the shared buffer so clients can map them without extra copies:

```bash
python examples/nn_weights/nn_server.py --listen 0.0.0.0:7040
```

In another terminal, run a client which reads the byte buffer using `torch.frombuffer`:

```bash
python examples/nn_weights/nn_client.py --server 0.0.0.0:7040
```

Watch the predictions converge toward the XOR truth table as the server continues training.
