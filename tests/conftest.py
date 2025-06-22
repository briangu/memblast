import socket
import contextlib
import pytest

@pytest.fixture
def free_tcp_port():
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('127.0.0.1', 0))
        addr, port = s.getsockname()
    return port
