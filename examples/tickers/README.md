# Ticker Examples

These scripts simulate streaming stock price data. Start the server and then run one of the clients.

## Files

- `ticker_server.py` – Publishes random prices for a list of tickers. The `--tickers` option controls which symbols are generated and `--window` sets the history length (defaults to one trading day).
- `ticker_client.py` – Connects to the server, reads the shared buffer and prints the rolling mean price for each ticker.
- `ticker_duckdb_client.py` – Similar to `ticker_client.py` but registers the shared array with DuckDB and uses SQL to compute averages.

## Usage

Start the server (default port 7011):

```bash
python examples/tickers/ticker_server.py --listen 0.0.0.0:7011
```

Connect with the basic client:

```bash
python examples/tickers/ticker_client.py --server 0.0.0.0:7011
```

Or run the DuckDB client:

```bash
python examples/tickers/ticker_duckdb_client.py --server 0.0.0.0:7011
```

Use `--tickers` and `--window` on any of the above commands to customise the data.

Each update includes metadata indicating the current index. The clients
use this value to determine how many rows of the history buffer contain
valid prices and compute statistics only over that slice.

