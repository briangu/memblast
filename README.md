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

The repository contains a simple server and two client examples located under
`examples/`.

Start a server in one terminal:
```bash
python examples/server.py --listen 0.0.0.0:7010
```

In another terminal, connect with a client:
```bash
python examples/client.py --peers 0.0.0.0:7010
```

`duckdb_client.py` demonstrates integrating with DuckDB and is executed in the
same way.

`ticker_server.py` and `ticker_client.py` stream random price updates for a
set of stock tickers. The client displays the rolling mean for each ticker.

Run the ticker server:
```bash
python examples/ticker_server.py --listen 0.0.0.0:7011
```

Connect with the ticker client:
```bash
python examples/ticker_client.py --peers 0.0.0.0:7011
```

## Running the tests

Ensure your virtual environment is active and the module is built as described
above, then run:
```bash
python -m pytest
```
