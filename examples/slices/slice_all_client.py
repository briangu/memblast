import argparse
import asyncio
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--server", default="0.0.0.0:7020")
args = parser.parse_args()

async def handle_update(node, meta):
    print("metadata", meta)
    with node.read() as arr:
        print("\033[H\033[J", end="")  # Move cursor to home position and clear screen
        print(arr)
        sys.stdout.flush()


async def main(node):
    await asyncio.Event().wait()


memblast.start("b", server=args.server, shape=[100, 5], main=main, on_update=handle_update)
