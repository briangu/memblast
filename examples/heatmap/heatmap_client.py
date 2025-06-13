import argparse
import time
import numpy as np
import memblast
import sys
import asyncio
import signal

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7050')
parser.add_argument('--size', type=int, default=128)
args = parser.parse_args()

size = args.size
chars = " .:-=+*#%@"

async def handle_update(node, meta):
    global size, chars
    print(meta)
    with node.read() as arr:
        arr = arr.reshape(size, size)
        norm = (arr - arr.min()) / (np.ptp(arr) + 1e-9)
        levels = (norm * (len(chars) - 1)).astype(int)
        print("\033[H\033[J", end="")
        for row in levels:
            print(''.join(chars[i] for i in row))
        sys.stdout.flush()


async def main() -> None:
    loop   = asyncio.get_running_loop()
    stop   = asyncio.Event()

    memblast.start('heatmap_client', server=args.server, shape=[size, size], on_update_async=handle_update, event_loop=loop)

    def _arm_shutdown(sig: signal.Signals):
        loop.create_task(_shutdown(sig))

    async def _shutdown(sig):
        print(f"\n💡 received {sig.name}, shutting down …")
        stop.set()                        # 1) unblock Quart
        await asyncio.gather(
            return_exceptions=True,
        )

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _arm_shutdown, sig)

    await stop.wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n💡 KeyboardInterrupt → shutdown request already handled.")
