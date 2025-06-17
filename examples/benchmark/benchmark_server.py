import argparse
import time
import numpy as np
import memblast

parser = argparse.ArgumentParser(description="Memblast benchmark server")
parser.add_argument('--listen', default='0.0.0.0:7040',
                    help='Base host:port to listen on. Each matrix size uses port+index')
parser.add_argument('--sizes', default='64,128,256',
                    help='Comma separated matrix sizes (NxN)')
parser.add_argument('--updates', type=int, default=1000,
                    help='Number of updates per matrix size')
parser.add_argument('--named', action='store_true',
                    help='Update only a named slice instead of the full matrix')
args = parser.parse_args()

sizes = [int(s) for s in args.sizes.split(',') if s]
host, base_port = args.listen.split(':')
base_port = int(base_port)

nodes = []
for idx, size in enumerate(sizes):
    listen = f"{host}:{base_port + idx}"
    node = memblast.start(f'bench_server_{size}', listen=listen, shape=[size, size])
    nodes.append(node)

    # wait for a client to signal readiness before beginning updates
    ready = False
    def meta_cb(m):
        nonlocal ready
        if m.get('ready'):
            ready = True
    node.on_update(meta_cb)
    print(f'waiting for client on {listen} (size {size})...')
    while not ready:
        # read processes incoming metadata
        with node.read():
            pass
        time.sleep(0.1)
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
    node.send_meta({'size': size, 'server_time': elapsed})
    node.flush(0)
    print(f'server size {size}: {args.updates / elapsed:.2f} updates/sec')
    time.sleep(1)

# signal completion using the last node
nodes[-1].send_meta({'done': True})
nodes[-1].flush(0)
print('Benchmark complete.')
while True:
    time.sleep(1)
