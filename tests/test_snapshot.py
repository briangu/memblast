import time
import raftmem


def test_snapshot_on_connect():
    node_a = raftmem.start("a", listen="127.0.0.1:7200", shape=[2])
    with node_a.write() as arr:
        arr[0] = 1.5
        arr[1] = 2.5
    time.sleep(1)

    meta = {}

    def cb(d):
        meta.update(d)

    node_b = raftmem.start("b", server="127.0.0.1:7200", shape=[2])
    node_b.on_update(cb)
    node_a.send_meta({"last_index": 1})
    node_a.flush(0)
    # allow time for snapshot to transfer
    time.sleep(2)
    with node_b.read() as arr:
        assert arr[0] == 1.5
        assert arr[1] == 2.5
    assert meta.get("last_index") == 1

