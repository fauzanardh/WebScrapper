import socket
import ssl

from http.client import HTTPResponse
from io import BytesIO


class FakeSocket:
    def __init__(self, response):
        self._file = BytesIO(response)

    def makefile(self, *args, **kwargs):
        return self._file


def get(host, port, uri):
    print(f"Getting data from: {host}:{port}{uri}")
    server_address = (host, port)
    message = b'GET %b HTTP/1.1\r\n' % uri.encode()
    message += b'Host: %b\r\n' % host.encode()
    message += b'Connection: close\r\n'
    message += b'\r\n'

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    context = ssl.create_default_context()
    ssl_sock = context.wrap_socket(sock, server_hostname=host)
    ssl_sock.connect(server_address)
    ssl_sock.sendall(message)

    data = b''
    while True:
        buf = ssl_sock.recv(1024)
        if not buf:
            break
        data += buf
    ssl_sock.close()
    data_buf = FakeSocket(data)
    response = HTTPResponse(data_buf)
    response.begin()
    content = response.read(len(data.decode()))
    print(f"Finished: {host}:{port}{uri}")
    return response, content
