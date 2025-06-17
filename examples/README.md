# Example Programs

This directory contains small programs demonstrating how to use **memblast**. Make sure the package is built with:

```bash
maturin develop --release
```

All commands below should be run from the repository root.

## Basic shared buffer

- `server.py` – Starts a server hosting a 10×10 array and updates a few random values every second.
- `client.py` – Connects to the server and prints the array along with any metadata updates.

Run them in separate terminals:

```bash
python examples/server.py --listen 0.0.0.0:7010
python examples/client.py --server 0.0.0.0:7010
```

## Slice examples

Code under [slices/](slices/) shows how clients can map slices of a large array. See `slices/README.md` for details.

## Ticker examples

The [tickers/](tickers/) folder streams fake stock ticker data. See `tickers/README.md` for the full description.

## Game of Life example

The [life/](life/) folder contains a 256×256 Conway game of life. Run the server and
use the client to view the whole grid or a sub-region with the `--region` option.

## Yahoo Finance example

The [yfinance/](yfinance/) folder pulls live data from Yahoo Finance and
registers the shared array with DuckDB. This mirrors the approach described in
[this article](https://www.defconq.tech/docs/tutorials/realTimeStocks?trk=feed_main-feed-card_feed-article-content).

## Neural network broadcast example

The [nn_weights/](nn_weights/) folder trains a small neural network on the XOR problem with PyTorch and streams the weights as raw bytes so clients can map them directly for inference.

## Heatmap example

The [heatmap/](heatmap/) folder streams a dynamic sine-wave heatmap. Clients render it as ASCII art.


## Benchmark example

The [benchmark/](benchmark/) directory measures update throughput for different
matrix sizes. Start the benchmark server first; it begins sending updates when a
client subscribes via the `on_connect` callback. Both scripts accept a comma
separated list of sizes and the number of updates to send for each size:

```bash
python examples/benchmark/benchmark_server.py --listen 0.0.0.0:7040 --sizes 128,256 --updates 1000
python examples/benchmark/benchmark_client.py --server 0.0.0.0:7040 --sizes 128,256 --updates 1000
```

Pass `--named` to benchmark updates on a named slice instead of the full matrix.
