"""真实 TCP + Protobuf/JSON + 独立 Redis 的集成测试；不清空用户 Redis。"""
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'build/generated'))
import protocol_pb2 as pb


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def packet(payload):
    return struct.pack('!I', len(payload)) + payload


def recv_exact(sock, count):
    chunks = bytearray()
    while len(chunks) < count:
        part = sock.recv(count - len(chunks))
        if not part:
            raise EOFError('connection closed')
        chunks.extend(part)
    return bytes(chunks)


def recv_frame(sock):
    size, = struct.unpack('!I', recv_exact(sock, 4))
    assert size <= 1024 * 1024
    return recv_exact(sock, size)


def request(kind, **fields):
    result = pb.ClientEnvelope(request_id=uuid.uuid4().hex)
    getattr(result, kind).SetInParent()
    for key, value in fields.items():
        setattr(getattr(result, kind), key, value)
    return result


class Peer:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=5)
        self.pending = []

    def send(self, message):
        self.sock.sendall(packet(message.SerializeToString()))

    def take(self, predicate):
        for index, message in enumerate(self.pending):
            if predicate(message):
                return self.pending.pop(index)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            self.sock.settimeout(max(.01, deadline - time.monotonic()))
            message = pb.ServerEnvelope.FromString(recv_frame(self.sock))
            if predicate(message):
                return message
            self.pending.append(message)
        raise TimeoutError('matching response not received')

    def call(self, message):
        self.send(message)
        return self.take(lambda value: value.request_id == message.request_id)

    def login(self, name, room):
        return self.call(request('login', nickname=name, channel_id=room))

    def close(self):
        self.sock.close()


class Integration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='tcp-chat-test-')
        cls.processes = []
        cls.logs = []
        cls.addClassCleanup(cls.cleanup)
        cls.redis_port, cls.data_port, cls.logic_port = free_port(), free_port(), free_port()
        # 极少数情况下 OS 返回同一端口，重新选择。
        while len({cls.redis_port, cls.data_port, cls.logic_port}) < 3:
            cls.redis_port, cls.data_port, cls.logic_port = free_port(), free_port(), free_port()
        cls.env = dict(os.environ, CHAT_REDIS_PORT=str(cls.redis_port),
                       CHAT_DATA_PORT=str(cls.data_port), CHAT_LOGIC_PORT=str(cls.logic_port),
                       CHAT_WORKERS='4', CHAT_IO_TIMEOUT_MS='700')
        cls.start(['redis-server', '--bind', '127.0.0.1', '--port', str(cls.redis_port),
                   '--save', '', '--appendonly', 'no', '--dir', cls.temp.name], cls.redis_port)
        cls.start([str(ROOT / 'bin/dataserver')], cls.data_port)
        cls.start([str(ROOT / 'bin/logicserver')], cls.logic_port)

    @classmethod
    def start(cls, command, port):
        log = tempfile.TemporaryFile(mode='w+b')
        cls.logs.append(log)
        process = subprocess.Popen(command, env=cls.env, stdout=log, stderr=log)
        cls.processes.append(process)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('server failed to start')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.1):
                    return
            except OSError:
                time.sleep(.03)
        raise TimeoutError('server startup timed out')

    @classmethod
    def cleanup(cls):
        for process in reversed(cls.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in cls.logs:
            log.seek(0)
            print(log.read().decode(errors='replace'))
            log.close()
        cls.temp.cleanup()

    def peer(self):
        peer = Peer(self.logic_port)
        self.addCleanup(peer.close)
        return peer

    def room(self):
        return uuid.uuid4().hex

    def test_login_broadcast_history_and_isolation(self):
        room = self.room()
        a, b, c = self.peer(), self.peer(), self.peer()
        self.assertTrue(a.login('a-' + room, room).login_response.success)
        self.assertEqual(b.login('a-' + room, room).error.code, 'NICKNAME_TAKEN')
        self.assertTrue(b.login('b-' + room, room).login_response.success)
        self.assertTrue(c.login('c-' + room, room + 'x').login_response.success)
        self.assertTrue(a.call(request('send_message', content='你好 TCP')).send_message_response.success)
        for peer in (a, b):
            message = peer.take(lambda value: value.HasField('chat_message'))
            self.assertEqual(message.chat_message.content, '你好 TCP')
        c.sock.settimeout(.15)
        with self.assertRaises(socket.timeout):
            c.sock.recv(1)
        d = self.peer()
        history = d.login('d-' + room, room).login_response.history
        self.assertEqual([m.content for m in history], ['你好 TCP'])
        self.assertEqual(b.call(request('send_message', content='')).error.code, 'INVALID_MESSAGE')
        self.assertTrue(b.call(request('quit')).quit_response.success)
        self.assertEqual(b.call(request('send_message', content='after quit')).error.code, 'NOT_LOGGED_IN')

    def test_fragmented_and_pipelined_requests_keep_order(self):
        a = self.peer()
        room = self.room()
        login = request('login', nickname=room, channel_id=room)
        first = packet(login.SerializeToString())
        for byte in first[:7]:
            a.sock.sendall(bytes([byte]))
        messages = [request('send_message', content=str(i)) for i in range(30)]
        a.sock.sendall(first[7:] + b''.join(packet(m.SerializeToString()) for m in messages))
        self.assertTrue(a.take(lambda m: m.request_id == login.request_id).login_response.success)
        for sent in messages:
            self.assertTrue(a.take(lambda m: m.request_id == sent.request_id).send_message_response.success)
        d = self.peer()
        history = d.login('reader-' + room, room).login_response.history
        self.assertEqual([m.content for m in history], [str(i) for i in range(30)])

    def test_history_is_capped_at_100(self):
        a = self.peer()
        room = self.room()
        self.assertTrue(a.login(room, room).login_response.success)
        for index in range(105):
            self.assertTrue(a.call(request('send_message', content=str(index))).send_message_response.success)
        b = self.peer()
        messages = b.login('reader-' + room, room).login_response.history
        self.assertEqual([m.content for m in messages], [str(i) for i in range(5, 105)])

    def test_disconnect_releases_nickname(self):
        room = self.room()
        a = self.peer()
        self.assertTrue(a.login(room, room).login_response.success)
        a.close()
        b = self.peer()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            response = b.login(room, room)
            if response.HasField('login_response'):
                self.assertTrue(response.login_response.success)
                break
            self.assertEqual(response.error.code, 'NICKNAME_TAKEN')
            time.sleep(.03)
        else:
            self.fail('nickname not released')

    def test_invalid_frames_and_json_do_not_kill_servers(self):
        a = self.peer()
        a.sock.sendall(struct.pack('!I', 1024 * 1024 + 1))
        self.assertEqual(a.sock.recv(1), b'')
        with socket.create_connection(('127.0.0.1', self.data_port), timeout=3) as sock:
            for bad in [b'{broken', b'[]', b'{"request_id":123}',
                        b'{"version":1,"request_id":"x","action":[],"channel_id":"x"}']:
                sock.sendall(packet(bad))
                self.assertFalse(json.loads(recv_frame(sock))['ok'])
            good = {'version': 1, 'request_id': 'valid', 'action': 'get_channel_history',
                    'channel_id': self.room()}
            sock.sendall(packet(json.dumps(good).encode()))
            self.assertTrue(json.loads(recv_frame(sock))['ok'])
        b = self.peer()
        self.assertTrue(b.login(self.room(), self.room()).login_response.success)

    def test_real_client_waits_for_quit(self):
        room = self.room()
        result = subprocess.run([str(ROOT / 'bin/client')], input=f'{room}\n{room}\nhello\n/quit\n',
                                text=True, capture_output=True, env=self.env, timeout=8)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('登录成功', result.stdout)
        self.assertNotIn('超时', result.stderr)
        b = self.peer()
        history = b.login('reader-' + room, room).login_response.history
        self.assertEqual([m.content for m in history], ['hello'])

    def test_z_downstream_failure_and_shutdown(self):
        # 最后一个测试停止数据服务器，检查错误响应及逻辑服务器仍然存活。
        self.processes[1].terminate()
        self.assertEqual(self.processes[1].wait(timeout=8), 0)
        a = self.peer()
        response = a.login(self.room(), self.room())
        self.assertEqual(response.error.code, 'DATA_UNAVAILABLE')
        self.assertIsNone(self.processes[2].poll())
        a.close()
        self.processes[2].terminate()
        self.assertEqual(self.processes[2].wait(timeout=8), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
