# Ticker Examples

These scripts simulate streaming stock price data. Each client passes async
`main` and `on_update` functions to `memblast.start` so updates trigger the
display automatically.

## Files

- `ticker_server.py` – Publishes random prices for a list of tickers. The `--tickers` option controls which symbols are generated and `--window` sets the history length.
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

