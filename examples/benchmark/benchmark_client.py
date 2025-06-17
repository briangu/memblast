import argparse
import asyncio
import time
import memblast

parser = argparse.ArgumentParser(description="Memblast benchmark client")
parser.add_argument(
    "--server", default="0.0.0.0:7040", help="Host:port of the benchmark server"
)
parser.add_argument(
    "--sizes", default="64,128,256", help="Comma separated matrix sizes (NxN)"
)
parser.add_argument(
    "--updates", type=int, default=1000, help="Number of updates per matrix size"
)
parser.add_argument(
    "--named",
    action="store_true",
    help="Subscribe to named slices instead of the full matrix",
)
args = parser.parse_args()

sizes = [int(s) for s in args.sizes.split(",") if s]
max_size = max(sizes)

maps = []
if args.named:
    for size in sizes:
        maps.append(([0, 0], [1, size], None, f"row0_{size}"))
else:
    for size in sizes:
        maps.append(([0, 0], [size, size], None, f"exp_{size}"))


async def run():
    loop = asyncio.get_running_loop()
    meta = {}

    async def on_update(node, m):
        meta.update(m)

    async def on_connect(node, info):
        print("connected:", info)

    async def on_disconnect(node, info):
        print("disconnected:", info)

    node = memblast.start(
        "bench_client",
        server=args.server,
        shape=[max_size, max_size],
        maps=maps,
        on_update_async=on_update,
        on_connect_async=on_connect,
        on_disconnect_async=on_disconnect,
        event_loop=loop,
    )

    results = []
    while True:
        msg = None
        while not msg:
            with node.read():
                if meta.get("experiment"):
                    msg = meta.copy()
                    meta.clear()
            await asyncio.sleep(0.01)
        exp = msg["experiment"]
        start_ver = node.version
        start = time.perf_counter()
        while node.version - start_ver < args.updates:
            with node.read():
                pass
            await asyncio.sleep(0.001)
        elapsed = time.perf_counter() - start
        results.append((exp, elapsed))
        print(f"{exp}: {args.updates / elapsed:.2f} updates/sec")
        if msg.get("done"):
            break
        await asyncio.sleep(0.5)

    print("Results:")
    for exp, ct in results:
        print(f"  {exp}: {args.updates / ct:.2f} updates/sec")


asyncio.run(run())
