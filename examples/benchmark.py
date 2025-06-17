import argparse
import time
import numpy as np
import memblast

parser = argparse.ArgumentParser()
parser.add_argument(
    "--sizes", default="64,128,256", help="Comma separated list of square matrix sizes"
)
parser.add_argument(
    "--duration", type=float, default=5.0, help="Seconds to run each benchmark per size"
)
args = parser.parse_args()

sizes = [int(s) for s in args.sizes.split(",") if s]


def run_bench(size: int) -> float:
    node = memblast.start(f"bench_{size}", shape=[size, size])
    count = 0
    start = time.time()
    while time.time() - start < args.duration:
        with node.write() as arr:
            arr = arr.reshape(size, size)
            arr.fill(float(count))
        count += 1
    return count / args.duration


for sz in sizes:
    rate = run_bench(sz)
    print(f"{sz}x{sz}: {rate:.2f} updates/sec")
