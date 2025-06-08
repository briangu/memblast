# Yahoo Finance Example

This folder demonstrates streaming live stock prices from Yahoo Finance into a
[memblast](../..) shared memory buffer. A DuckDB client can then query the data
using SQL. The design mirrors the approach in the article ["A Real Time Stock Market Feed"](https://www.defconq.tech/docs/tutorials/realTimeStocks?trk=feed_main-feed-card_feed-article-content) which implements a similar feed with KDB/Q.

## Files

- `yfinance_server.py` – Fetches real prices with the `yfinance` library and
  updates a circular window in shared memory.
- `yfinance_duckdb_client.py` – Connects to the server, registers the live array
  with DuckDB and computes averages via SQL.

## Usage

Build the package first (from the repository root):

```bash
maturin develop --release
```

Start the server (default port 7012):

```bash
python examples/yfinance/yfinance_server.py --listen 0.0.0.0:7012
```

Run the DuckDB client:

```bash
python examples/yfinance/yfinance_duckdb_client.py --server 0.0.0.0:7012
```

Both scripts accept `--tickers` and `--window` arguments. `--tickers` can be a
comma-separated list or a path to a CSV file containing one symbol per line.
