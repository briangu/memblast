import time
import raftmem


def test_snapshot_on_connect():
    node_a = raftmem.start("a", listen="127.0.0.1:7200", shape=[2])
    with node_a.write() as arr:
        arr[0] = 1.5
        arr[1] = 2.5
    time.sleep(1)

    node_b = raftmem.start("b", server="127.0.0.1:7200", shape=[2])
    # allow time for snapshot to transfer
    time.sleep(2)
    with node_b.read() as arr:
        assert arr[0] == 1.5
        assert arr[1] == 2.5

