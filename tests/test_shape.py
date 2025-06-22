import memblast


def test_write_guard_shape():
    node = memblast.start("sh", shape=[2, 3])
    with node.write() as arr:
        assert arr.shape == (2, 3)
        arr[0, 0] = 1.0
    with node.read() as arr:
        assert arr.shape == (2, 3)
        assert arr[0, 0] == 1.0
