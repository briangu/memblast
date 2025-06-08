# RDMA Backend Tasks

1. **Create RDMA transport layer**
   - Add a new `src/rdma.rs` mirroring the `net` module. It should expose `serve` and `client` functions with the same signatures as those defined in `src/net.rs` around lines 458-533【F:src/net.rs†L458-L533】.
   - Include any required crates in `Cargo.toml` (for example an RDMA library).

2. **Extend `start()` API**
   - Update the function defined at `src/lib.rs` lines 235‑244 to accept a `backend` argument with a default such as `"tcp"`【F:src/lib.rs†L235-L244】.
   - Based on the `backend` value spawn either `net::serve/client` or the new `rdma::serve/client`. The current TCP implementation is started in lines 274‑298 of `start()`【F:src/lib.rs†L274-L298】.

3. **Expose the option to Python**
   - Modify the `#[pyo3(signature = ...)]` annotation so that Python callers can pass `backend="rdma"` when invoking `memblast.start`.
   - Update examples (e.g. `examples/server.py` lines 8‑13【F:examples/server.py†L8-L13】) to demonstrate selecting the backend.

4. **Add unit tests**
   - Create tests similar to `tests/test_snapshot.py` that start a server and client using the RDMA path and verify data transfer【F:tests/test_snapshot.py†L1-L24】.

5. **Update documentation**
   - Document the new backend parameter in `README.md` including build notes (compile with `maturin develop --release` per lines 16‑18)【F:README.md†L16-L18】 and describe how to run with RDMA.

