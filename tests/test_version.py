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
