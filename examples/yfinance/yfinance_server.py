import argparse
import asyncio
import memblast
import numpy as np
import yfinance as yf
import os
import sys


def load_tickers(spec: str):
    if os.path.isfile(spec):
        with open(spec) as f:
            return [line.strip() for line in f if line.strip()]
    return [t.strip() for t in spec.split(',') if t.strip()]


parser = argparse.ArgumentParser()
parser.add_argument('--listen', default='0.0.0.0:7012')
parser.add_argument('--tickers', default='AAPL,GOOG,MSFT')
parser.add_argument('--window', type=int, default=5)
parser.add_argument('--interval', type=int, default=30)
args = parser.parse_args()

tickers = load_tickers(args.tickers)
window = args.window
node = memblast.start('yfinance_server', listen=args.listen, shape=[len(tickers), window])

index = 0


async def main():
    global index
    while True:
        try:
            data = yf.download(' '.join(tickers), period='1d', interval='1m', progress=False, group_by='ticker')
        except Exception as e:
            print('download failed', e)
            await asyncio.sleep(args.interval)
            continue

    prices = []
    if len(tickers) == 1:
        prices.append(float(data['Close'].iloc[-1]))
    else:
        for t in tickers:
            prices.append(float(data[t]['Close'].iloc[-1]))

    with node.write() as arr:
        arr = arr.reshape(len(tickers), window)
        for i, price in enumerate(prices):
            arr[i, index % window] = price
        node.send_meta({'index': index})

    with node.read() as arr:
        arr = np.array(arr).reshape(len(tickers), window)
        print("\033[H\033[J", end="")
        for t, row in zip(tickers, arr):
            print(t, row)
        sys.stdout.flush()

        index += 1
        await asyncio.sleep(args.interval)


asyncio.run(main())
