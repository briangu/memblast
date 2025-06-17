import argparse
import asyncio
import time
import numpy as np
import memblast

parser = argparse.ArgumentParser(description="Memblast benchmark server")
parser.add_argument('--listen', default='0.0.0.0:7040',
                    help='Host:port to listen on')
parser.add_argument('--sizes', default='64,128,256',
                    help='Comma separated matrix sizes (NxN)')
parser.add_argument('--updates', type=int, default=1000,
                    help='Number of updates per matrix size')
parser.add_argument('--named', action='store_true',
                    help='Update only a named slice instead of the full matrix')
args = parser.parse_args()

sizes = [int(s) for s in args.sizes.split(',') if s]
listen = args.listen

async def run():
    loop = asyncio.get_running_loop()

    for idx, size in enumerate(sizes):
        ready = asyncio.Event()

        async def conn_cb(node, info):
            print('client subscribed:', info)
            ready.set()

        async def disc_cb(node, info):
            print('client disconnected:', info)

        node = memblast.start(
            f'bench_server_{size}',
            listen=listen,
            shape=[size, size],
            on_connect_async=conn_cb,
            on_disconnect_async=disc_cb,
            event_loop=loop,
        )

        print(f'waiting for client on {listen} (size {size})...')
        while not ready.is_set():
            await asyncio.sleep(0.1)
        print('client connected - starting updates')

        start = time.perf_counter()
        for _ in range(args.updates):
            with node.write() as arr:
                arr = arr.reshape(size, size)
                if args.named:
                    arr[0, :] = np.random.random(size)
                else:
                    arr[:, :] = np.random.random((size, size))
        elapsed = time.perf_counter() - start
        meta = {'size': size, 'server_time': elapsed}
        if idx == len(sizes) - 1:
            meta['done'] = True
        node.send_meta(meta)
        node.flush(0)
        print(f'server size {size}: {args.updates / elapsed:.2f} updates/sec')
        await asyncio.sleep(1)
        node.close()

    print('Benchmark complete.')

asyncio.run(run())
