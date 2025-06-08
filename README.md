# memblast

This project exposes a small Raft-backed shared memory buffer as a Python extension module.

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

## Running the tests

Ensure your virtual environment is active and the module is built as described
above, then run:
```bash
python -m pytest
```
