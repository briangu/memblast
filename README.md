# memblast

`memblast` is a high-performance publish/subscribe ticker plant built around a shared memory buffer.
Updates are replicated across nodes and exposed as NumPy arrays for lightning-fast Python access.

### Features

- `version` property to track updates on each node
- Subscribe to named sub regions and access them via `node.ndarray(name)`
- Send and receive JSON metadata with synchronous or async callbacks

### In progress

- Switchable network layer (TCP or RDMA)
- Metrics collection
- Snapshots and MVCC

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
Alternatively, a simple Quart-based WebSocket client is provided:
```bash
python examples/client_ws.py --server 0.0.0.0:7010
```

Additional example categories live under `examples/slices` and
`examples/tickers`. The ticker examples stream random price updates for a set of
stock tickers and the clients compute rolling statistics.

There is also a Yahoo Finance example under `examples/yfinance` that pulls
real data using the `yfinance` library and demonstrates querying the live
buffer with DuckDB. See `examples/yfinance/README.md` for details.

Run the ticker server:
```bash
python examples/tickers/ticker_server.py --listen 0.0.0.0:7011
```

Connect with the ticker client:
```bash
python examples/tickers/ticker_client.py --server 0.0.0.0:7011
```

The `examples/life` directory runs a Conway game of life. The server defaults to
a 64×64 world but the size is configurable with `--width`. Start the server with
`python examples/life/life_server.py` and connect using
`python examples/life/life_client.py`. Pass `--region` to the client to view a
sub-region of the world.
## Running the tests

Ensure your virtual environment is active and the module is built as described
above, then run:
```bash
python -m pytest
```

