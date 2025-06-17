import argparse
import time
import memblast

parser = argparse.ArgumentParser(description="Memblast benchmark client")
parser.add_argument('--server', default='0.0.0.0:7040',
                    help='Base host:port of the benchmark server')
parser.add_argument('--sizes', default='64,128,256',
                    help='Comma separated matrix sizes (NxN)')
parser.add_argument('--updates', type=int, default=1000,
                    help='Number of updates per matrix size')
parser.add_argument('--named', action='store_true',
                    help='Subscribe to a named slice instead of the full matrix')
args = parser.parse_args()

sizes = [int(s) for s in args.sizes.split(',') if s]
host, base_port = args.server.split(':')
base_port = int(base_port)

results = []

for idx, size in enumerate(sizes):
    port = base_port + idx
    if args.named:
        maps = [([0, 0], [1, size], [0, 0], 'row0')]
        node = memblast.start(f'bench_client_{size}', server=f'{host}:{port}',
                              shape=[1, size], maps=maps)
    else:
        node = memblast.start(f'bench_client_{size}', server=f'{host}:{port}',
                              shape=[size, size])

    meta = {}
    def handle(m):
        meta.update(m)
    node.on_update(handle)

    # signal the server that this client is ready
    node.send_meta({'ready': True})
    node.flush(0)

    start_ver = node.version
    start = time.perf_counter()
    while node.version - start_ver < args.updates:
        time.sleep(0.001)
    client_time = time.perf_counter() - start

    while 'server_time' not in meta:
        with node.read():
            pass
        time.sleep(0.01)
    server_time = meta['server_time']
    results.append((size, server_time, client_time))
    print(f'size {size}: server {server_time:.4f}s client {client_time:.4f}s')
    if meta.get('done'):
        break
    time.sleep(0.5)

print('Results:')
for size, st, ct in results:
    rate = args.updates / max(st, ct)
    print(f'  {size}x{size}: {rate:.2f} updates/sec')
