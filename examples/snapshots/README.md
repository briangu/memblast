# Snapshot MVCC Example

This folder demonstrates using snapshots with **memblast** and DuckDB.
A small server updates an array every second and records a snapshot after
each update. The DuckDB client fetches two versions of the data and
compares their means using SQL.

## Files

- `snapshot_server.py` – Hosts a one-dimensional array and takes a snapshot
  after every update.
- `snapshot_duckdb_client.py` – Connects to the server, retrieves two
  snapshots and computes their means with DuckDB.

## Usage

Start the server (default port 7013):

```bash
python examples/snapshots/snapshot_server.py --listen 0.0.0.0:7013
```

In another terminal fetch and compare two snapshots:

```bash
python examples/snapshots/snapshot_duckdb_client.py --server 0.0.0.0:7013 \
    --snap1 3 --snap2 5
```
