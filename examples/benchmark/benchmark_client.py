import argparse
import asyncio
import time
import memblast

parser = argparse.ArgumentParser(description="Memblast benchmark client")
parser.add_argument(
    "--server", default="0.0.0.0:7040", help="Host:port of the benchmark server"
)
parser.add_argument(
    "--size", default=256, type=int, help="Matrix size (NxN)"
)
parser.add_argument(
    "--updates", type=int, default=1000, help="Number of updates per matrix size"
)
args = parser.parse_args()

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
        shape=[args.size, args.size],
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
