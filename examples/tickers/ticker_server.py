import argparse
import asyncio
import random
import memblast
import sys

parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7011')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=5)
args = parser.parse_args()

tickers = args.tickers.split(',')
window = args.window
loop = asyncio.get_event_loop()
node = memblast.start("ticker_server", listen=args.listen, shape=[len(tickers), window], event_loop=loop)

index = 0


async def main():
    global index
    while True:
        with node.write() as arr:
            arr = arr.reshape(len(tickers), window)
            for i in range(len(tickers)):
                arr[i, index % window] = random.uniform(100.0, 200.0)
            node.send_meta({'index': index})
        with node.read() as data:
            data = data.reshape(len(tickers), window)
            print("\033[H\033[J", end="")
            for t, row in zip(tickers, data):
                print(t, row)
            sys.stdout.flush()
        await asyncio.sleep(1)
        index += 1


loop.create_task(main())
loop.run_forever()
