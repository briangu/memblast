import memblast


def test_version_increments():
    node = memblast.start("ver", shape=[1])
    assert node.version == 0
    with node.write() as arr:
        arr[0] = 1.0
    assert node.version == 1
    with node.write() as arr:
        arr[0] = 2.0
    assert node.version == 2


def test_network_version_updates():
    node_a = memblast.start("va", listen="127.0.0.1:7300", shape=[1])
    node_b = memblast.start("vb", server="127.0.0.1:7300", shape=[1])
    import time
    time.sleep(1)
    assert node_b.version == 0
    with node_a.write() as arr:
        arr[0] = 3.14
    time.sleep(1)
    assert node_a.version == 1
    assert node_b.version == 1
