# Conway's Game of Life

This example runs a Conway game of life simulation using **memblast**. The
server maintains the world state and clients subscribe to updates. By default
the world is 64×64 but the size can be adjusted.

## Files

- `life_server.py` – Hosts the world and updates it every half second. Pass
  `--width` to change the grid size (default 64).
- `life_client.py` – Displays the world. Pass `--region row,col,h,w` to view
  only a portion of the grid.

## Usage

Start the server (default port 7030):

```bash
python examples/life/life_server.py --listen 0.0.0.0:7030
```

View the entire world:

```bash
python examples/life/life_client.py --server 0.0.0.0:7030
```

Or view a sub-region, e.g. a 50×50 section starting at (100,100):

```bash
python examples/life/life_client.py --server 0.0.0.0:7030 --region 100,100,50,50
```
