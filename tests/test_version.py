import time
import pytest
import memblast


def test_version_increments():
    node = memblast.start("ver", shape=[1])
    assert node.version[node.name] == 0
    with node.write() as arr:
        arr[0] = 1.0
    assert node.version[node.name] == 1
    with node.write() as arr:
        arr[0] = 2.0
    assert node.version[node.name] == 2


def test_network_version_updates(free_tcp_port):
    port = free_tcp_port
    addr = f"127.0.0.1:{port}"
    node_a = memblast.start("va", listen=addr, shape=[1])
    node_b = memblast.start("vb", server=addr, shape=[1])
    time.sleep(1)
    assert node_b.version[addr] == 0
    with node_a.write() as arr:
        arr[0] = 3.14
    time.sleep(1)
    assert node_a.version[node_a.name] == 1
    assert node_b.version[addr] == 1


def test_version_meta_requires_write():
    node = memblast.start("mx", shape=[1])
    with pytest.raises(RuntimeError):
        node.version_meta({"bad": True})
