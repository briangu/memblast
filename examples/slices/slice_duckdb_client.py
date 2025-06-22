import argparse
import memblast
import duckdb

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='0.0.0.0:7020')
parser.add_argument('--tickers', default='2,50,75')
parser.add_argument('--window', type=int, default=5)
args = parser.parse_args()

tickers = [int(t) for t in args.tickers.split(',') if t]

maps = []
for i, t in enumerate(tickers):
    maps.append(([t, 0], [1, args.window], [i, 0], None))

window = args.window

con = duckdb.connect()
query = 'SELECT ' + ', '.join(f'AVG(column{i})' for i in range(len(tickers))) + ' FROM data'
initialized = False

async def handle_update(node, meta):
    global initialized
    if not initialized:
        data = node.ndarray()
        con.register('data', data)
        initialized = True
    with node.read() as arr:
        data = arr
        print(data.shape)
        result = con.execute(query).fetchall()[0]
        print("\033[H\033[J", end="")
        print(result)

memblast.start(
    'slice_duckdb_client',
    server=args.server,
    shape=[len(tickers), window],
    maps=maps,
    on_update_async=handle_update,
)
