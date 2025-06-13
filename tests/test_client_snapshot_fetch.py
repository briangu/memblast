import time
import memblast


def test_client_fetch_versions():
    server = memblast.start("srv", listen="127.0.0.1:7300", shape=[2], snapshots=True)
    with server.write() as arr:
        arr[0] = 1.0
        arr[1] = 2.0
    server.take_snapshot()
    with server.write() as arr:
        arr[0] = 3.0
    with server.write() as arr:
        arr[1] = 4.0
    time.sleep(1)

    client = memblast.start("cli", server="127.0.0.1:7300", shape=[2], snapshots=True)
    time.sleep(2)

    assert client.get_version(1) == [1.0, 2.0]
    assert client.get_version(2) == [3.0, 2.0]
    assert client.get_version(3) == [3.0, 4.0]
