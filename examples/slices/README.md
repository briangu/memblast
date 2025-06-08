# Slice Examples

These programs demonstrate mapping portions of a large shared array to clients.
A server maintains a `(tickers x window)` buffer of random data. Each client
selects slices of that buffer in different ways.

## Files

- `slice_server.py` – Hosts the full array. The number of tickers and the size of the history window can be configured.
- `slice_client.py` – Maps specific ticker rows into its own array using the mapping API and prints them.
- `slice_all_client.py` – Connects to the server and views the entire dataset without slicing.
- `slice_named_client.py` – Attaches named slices so they can be retrieved later with `node.ndarray(name)`.
- `slice_duckdb_client.py` – Registers the mapped array with DuckDB and performs SQL queries over the live data.

## Usage

Start the server (default port 7020):

```bash
python examples/slices/slice_server.py --listen 0.0.0.0:7020
```

Run a basic slice client:

```bash
python examples/slices/slice_client.py --server 0.0.0.0:7020 --tickers 2,50,75
```

To view the full table:

```bash
python examples/slices/slice_all_client.py --server 0.0.0.0:7020
```

Named slice client:

```bash
python examples/slices/slice_named_client.py --server 0.0.0.0:7020 --tickers 2,50,75
```

DuckDB slice client:

```bash
python examples/slices/slice_duckdb_client.py --server 0.0.0.0:7020 --tickers 2,50,75
```

Use `--tickers` and `--window` on the server and clients to change the data shape.

