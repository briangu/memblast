# Heatmap Example

This demo streams a dynamic 2D heatmap generated from sine waves. The server
updates the array every 100ms and clients render the values as ASCII art.

## Files

- `heatmap_server.py` – Generates the heatmap and publishes it.
- `heatmap_client.py` – Connects to the server and displays the data.

## Usage

Build the package first if needed:

```bash
maturin develop --release
```

Start the server (default port 7050):

```bash
python examples/heatmap/heatmap_server.py --listen 0.0.0.0:7050
```

In another terminal, run a client:

```bash
python examples/heatmap/heatmap_client.py --server 0.0.0.0:7050
```

Try changing `--size` on both scripts to use a larger array.
