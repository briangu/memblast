import memblast


def test_get_version_after_snapshot():
    node = memblast.start("sm", shape=[2], snapshots=True)
    with node.write() as arr:
        arr[0] = 1.0
        arr[1] = 2.0
    node.take_snapshot()
    with node.write() as arr:
        arr[0] = 3.0
    with node.write() as arr:
        arr[1] = 4.0

    assert node.get_version(0) is None
    assert node.get_version(1) == [1.0, 2.0]
    assert node.get_version(2) == [3.0, 2.0]
    assert node.get_version(3) == [3.0, 4.0]


def test_get_version_without_snapshot_returns_none():
    node = memblast.start("sm2", shape=[2], snapshots=True)
    with node.write() as arr:
        arr[0] = 5.0
        arr[1] = 6.0
    # no explicit snapshot taken
    assert node.get_version(1) is None



