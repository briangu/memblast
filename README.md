# memblast

**memblast** exposes a memory-mapped NumPy array that can be shared between
processes and replicated over the network. A built-in server broadcasts
incremental updates to connected clients, which can subscribe to arbitrary
slices or named arrays and optionally verify hashes of each snapshot. Example
programs demonstrate streaming stock tickers, synchronizing a Conway game of
life, broadcasting PyTorch weights, rendering dynamic heatmaps, bridging to a
browser via WebSockets, and running a peer-to-peer mesh.

## Setup

1. Create and activate a Python virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install the Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Build the Rust extension in editable mode using `maturin`:
   ```bash
   maturin develop --release
   ```


## Building wheels for PyPI

A GitHub Actions workflow builds and uploads wheels for Linux, macOS and Windows on x86_64 and aarch64 architectures. Push a tag like `v0.1.0` with the `PYPI_API_TOKEN` secret set to publish.

## Running the examples

The repository contains a number of example programs under `examples/`. The
basic example is a simple server and client pair.

Start a server in one terminal:
```bash
python examples/server.py --listen 0.0.0.0:7010
```

In another terminal, connect with a client:
```bash
python examples/client.py --server 0.0.0.0:7010
```

Additional example categories live under `examples/slices`, `examples/tickers`,
`examples/nn_weights`, `examples/heatmap`, and `examples/yfinance`. The ticker
examples stream random price updates for a set of stock tickers and the clients
compute rolling statistics. The neural network and heatmap folders showcase
streaming PyTorch weights and a dynamic heatmap. The Yahoo Finance example pulls
real prices using the `yfinance` library and demonstrates querying the live
buffer with DuckDB. See each subdirectory's README for details.

Run the ticker server:
```bash
python examples/tickers/ticker_server.py --listen 0.0.0.0:7011
```

Connect with the ticker client:
```bash
python examples/tickers/ticker_client.py --server 0.0.0.0:7011
```

The `examples/life` directory runs a 256×256 Conway game of life. Start the server
with `python examples/life/life_server.py` and connect using
`python examples/life/life_client.py`. Pass `--region` to the client to view a
sub-region of the world.

For a peer-to-peer setup where each process owns a portion of the array and
receives updates for the rest, use `examples/peer_split.py`. The script uses the
`servers` parameter to connect to other peers while listening for incoming
connections. Each peer writes a running counter modulo 4 into its quadrant so
the origin of each region is visible. Launch four peers (`a` through `d`) to
divide the buffer into quadrants:

```bash
python examples/peer_split.py --name a
python examples/peer_split.py --name b
python examples/peer_split.py --name c
python examples/peer_split.py --name d
```

`examples/peer_stress.py` runs a similar peer network but repeatedly writes to
stress test distributed updates.

`examples/client_ws.py` exposes the buffer over a WebSocket so updates can be
viewed in a browser.

## Running the tests

Ensure your virtual environment is active and the module is built as described
above, then run:
```bash
python -m pytest
```

## Hash verification

Clients can optionally request an integrity check on snapshot updates. Pass
`check_hash=True` to `memblast.start()` to have the server include a SHA‑256
hash of each snapshot and validate it on receipt. The parameter defaults to
`False` so there is no extra hashing overhead unless explicitly enabled.

