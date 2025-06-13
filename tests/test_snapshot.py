import time
import memblast


def test_snapshot_on_connect():
    node_a = memblast.start("a", listen="127.0.0.1:7200", shape=[2])
    with node_a.write() as arr:
        arr[0] = 1.5
        arr[1] = 2.5
    time.sleep(1)

    meta = {}

    def cb(d):
        meta.update(d)

    node_b = memblast.start("b", server="127.0.0.1:7200", shape=[2], on_update=cb, block=False)
    node_a.send_meta({"last_index": 1})
    node_a.flush(0)
    # allow time for snapshot to transfer
    time.sleep(2)
    with node_b.read() as arr:
        assert arr[0] == 1.5
        assert arr[1] == 2.5
    assert meta.get("last_index") == 1



def test_slice_snapshot():
    node_a = memblast.start("sa", listen="127.0.0.1:7201", shape=[4])
    with node_a.write() as arr:
        arr[0] = 1.0
        arr[1] = 2.0
        arr[2] = 3.0
        arr[3] = 4.0
    time.sleep(1)

    maps = [([1], [2], [0], None)]
    node_b = memblast.start("sb", server="127.0.0.1:7201", shape=[2], maps=maps)
    time.sleep(2)
    with node_b.read() as arr:
        assert list(arr) == [2.0, 3.0]

    with node_a.write() as arr:
        arr[1] = 5.0
        arr[2] = 6.0
    time.sleep(2)
    with node_b.read() as arr:
        assert list(arr) == [5.0, 6.0]
