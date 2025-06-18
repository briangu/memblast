import argparse
import asyncio
import time
import numpy as np
import memblast

parser = argparse.ArgumentParser(description="Memblast benchmark server")
parser.add_argument("--listen", default="0.0.0.0:7040", help="Host:port to listen on")
parser.add_argument(
    "--size", default="256", help="Matrix dimension (square)", type=int
)
parser.add_argument(
    "--updates", type=int, default=1000, help="Number of updates per matrix size"
)
args = parser.parse_args()

async def run(size: int):
    loop = asyncio.get_running_loop()
    ready = asyncio.Event()

    async def on_connect(node, info):
        print("client subscribed:", info)
        ready.set()

    node = memblast.start(
        "bench_server",
        listen=args.listen,
        shape=[size, size],
        on_connect_async=on_connect,
        event_loop=loop,
    )

    print(f"waiting for client on {args.listen}...")
    await ready.wait()
    print("client connected - starting benchmarks")

    for idx, size in enumerate([size]):
        total_time_s = 0
        meta = {
            "experiment": f"{size}x{size}",
            "done": False,
        }
        for i in range(args.updates):
            # random is expensive so do it outside the timer
            vals = np.random.random((size, size))
            start = time.perf_counter()
            with node.write() as arr:
                node.version_meta(meta)
                arr = arr.reshape(size, size)
                arr[:size, :size] = vals
            total_time_s += time.perf_counter() - start
        # updates / second
        print(f"{size}x{size}: {args.updates / total_time_s:.2f} updates/sec", end="")
        total_bytes = size * size * 8
        # bytes / second
        print(f" {total_bytes / total_time_s / (2 ** 20):.2f} MB/s", end="\n")

    node.close()
    print("Benchmark complete.")


asyncio.run(run(args.size))
