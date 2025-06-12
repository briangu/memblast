import subprocess
import sys
import time
import memblast

PORT = "127.0.0.1:7300"

SERVER_SCRIPT = """
import sys, time, memblast
addr = sys.argv[1]
val = float(sys.argv[2])
node = memblast.start('srv', listen=addr, shape=[1])
with node.write() as arr:
    arr[0] = val
node.flush(0)
time.sleep(3)
"""


def run_server(val):
    return subprocess.Popen([
        sys.executable,
        "-c",
        SERVER_SCRIPT,
        PORT,
        str(val),
    ])


def test_reconnect():
    srv = run_server(1.0)
    time.sleep(1)
    node = memblast.start('cli', server=PORT, shape=[1])
    time.sleep(1)
    with node.read() as arr:
        assert arr[0] == 1.0
    srv.wait()
    srv = run_server(2.0)
    time.sleep(4)
    with node.read() as arr:
        assert arr[0] == 2.0
    srv.terminate()
