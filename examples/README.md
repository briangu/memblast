# Example Programs

This directory contains small programs demonstrating how to use **memblast**.
All examples are event driven. Each script passes an async `main` coroutine and
an `on_update` callback to `memblast.start`, which uses an asyncio event loop to
invoke the handler whenever updates arrive. You can supply your own loop or let
`start` spawn one for you.
Make sure the package is built with:

```bash
maturin develop --release
```

All commands below should be run from the repository root.

## Basic shared buffer

- `server.py` – Starts a server hosting a 10×10 array and updates a few random values every second.
- `client.py` – Connects to the server and prints the array whenever the update callback fires.

Run them in separate terminals:

```bash
python examples/server.py --listen 0.0.0.0:7010
python examples/client.py --server 0.0.0.0:7010
```

## Slice examples

Code under [slices/](slices/) shows how clients can map slices of a large array. See `slices/README.md` for details.

## Ticker examples

The [tickers/](tickers/) folder streams fake stock ticker data. See `tickers/README.md` for the full description.

## Yahoo Finance example

The [yfinance/](yfinance/) folder pulls live data from Yahoo Finance and
registers the shared array with DuckDB. This mirrors the approach described in
[this article](https://www.defconq.tech/docs/tutorials/realTimeStocks?trk=feed_main-feed-card_feed-article-content).

## Websocket demo

`flask_ws.py` starts a small Quart-based web server and streams updates to a
browser over a WebSocket.

