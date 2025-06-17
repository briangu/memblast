import argparse
import asyncio
import time
import numpy as np
import memblast

parser = argparse.ArgumentParser(description="Memblast benchmark server")
parser.add_argument("--listen", default="0.0.0.0:7040", help="Host:port to listen on")
parser.add_argument(
    "--sizes", default="64,128,256", help="Comma separated matrix sizes (NxN)"
)
parser.add_argument(
    "--updates", type=int, default=1000, help="Number of updates per matrix size"
)
parser.add_argument(
    "--named",
    action="store_true",
    help="Update only a named slice instead of the full matrix",
)
args = parser.parse_args()

sizes = [int(s) for s in args.sizes.split(",") if s]
max_size = max(sizes)


async def run():
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()

    async def on_connect(node, info):
        print("client subscribed:", info)
        ready.set()

    node = memblast.start(
        "bench_server",
        listen=args.listen,
        shape=[max_size, max_size],
        on_connect_async=on_connect,
        event_loop=loop,
    )

    print(f"waiting for client on {args.listen}...")
    await ready.wait()
    print("client connected - starting benchmarks")

    for idx, size in enumerate(sizes):
        start = time.perf_counter()
        for i in range(args.updates):
            vals = np.random.random(size) if args.named else np.random.random((size, size))
            with node.write() as arr:
                node.version_meta({
                    "experiment": f"{size}x{size}",
                    "done": i == args.updates - 1 and idx == len(sizes) - 1,
                })
                arr = arr.reshape(max_size, max_size)
                if args.named:
                    arr[0, :size] = vals
                else:
                    arr[:size, :size] = vals
        elapsed = time.perf_counter() - start
        print(f"{size}x{size}: {args.updates / elapsed:.2f} updates/sec")
        await asyncio.sleep(1)

    node.close()
    print("Benchmark complete.")


asyncio.run(run())
