"""Local TCP integration of GFD, three LFDs, and three replicas.

Run: python -m unittest discover -s tests -p test_gfd_integration.py -v
"""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]

class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.env = os.environ.copy()
        self.env['PYTHONPATH'] = str(ROOT / 'src')
        self.processes = []
        self.logs = {}
        self.ports = {}
        held = []
        for name in ['GFD', 'LFD1', 'LFD2', 'LFD3', 'S1', 'S2', 'S3']:
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            held.append(sock)
            self.ports[name] = sock.getsockname()[1]
            self.env[name + '_HOST'] = '127.0.0.1'
            self.env[name + '_PORT'] = str(self.ports[name])
        for sock in held:
            sock.close()

    def tearDown(self):
        for process in self.processes:
            if process.poll() is None:
                process.send_signal(signal.SIGCONT)
                process.terminate()
        for process in self.processes:
            process.wait(timeout=5)
        for stream in self.logs.values():
            stream.close()

    def launch(self, label, component, *args):
        stream = tempfile.TemporaryFile(mode='w+')
        self.logs[label] = stream
        process = subprocess.Popen(
            [sys.executable, '-u', '-m', f'distributed_system.{component}.{component}', *args],
            cwd=ROOT, env=self.env, stdout=stream, stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    def output(self, name):
        stream = self.logs[name]
        stream.seek(0)
        return stream.read()

    def wait_for(self, name, text, count=1):
        deadline = time.monotonic() + 7
        while time.monotonic() < deadline:
            if self.output(name).count(text) >= count:
                return
            time.sleep(.03)
        self.fail(f'Missing {text!r} ({count}) in {name}:\n{self.output(name)}')

    def gfd(self, label='GFD'):
        return self.launch(label, 'gfd', '--freq', '10', '--timeout', '.5')

    def request(self, replica):
        with socket.create_connection(('127.0.0.1', self.ports[replica]), timeout=2) as sock:
            sock.sendall((json.dumps(dict(type='request', client_id='C1', replica_id=replica,
                                         request_num=1, payload='Hello')) + '\n').encode())
            with sock.makefile('rb') as stream:
                reply = json.loads(stream.readline())
            self.assertEqual(reply['replica_id'], replica)
            self.assertEqual(reply['request_num'], 1)

    def test_full_lifecycle(self):
        gfd = self.gfd()
        self.wait_for('GFD', 'GFD: 0 members')
        lfds = {}
        for n in (1, 2, 3):
            name = f'LFD{n}'
            lfds[n] = self.launch(name, 'lfd', '--id', name, '--freq', '10', '--timeout', '.5')
            self.wait_for('GFD', f'GFD receives heartbeat from {name}', 2)
        self.assertNotIn('add replica', self.output('GFD'))
        servers = {}
        for n in (1, 2, 3):
            name = f'S{n}'
            servers[n] = self.launch(name, 'server', '--id', name)
            self.wait_for('GFD', f'LFD{n}: add replica {name}')
            self.request(name)
        self.wait_for('GFD', 'GFD: 3 members: S1, S2, S3')
        for n in (1, 2, 3):
            self.assertEqual(self.output('GFD').count(f'LFD{n}: add replica S{n}'), 1)

        # A live but unresponsive server times out without starving GFD traffic.
        before = self.output('GFD').count('GFD receives heartbeat from LFD1')
        servers[1].send_signal(signal.SIGSTOP)
        self.wait_for('GFD', 'LFD1: delete replica S1')
        self.wait_for('GFD', 'GFD receives heartbeat from LFD1', before + 3)
        self.assertIsNone(lfds[1].poll())
        servers[1].kill()
        servers[1].wait(timeout=3)
        servers[2].terminate()
        servers[2].wait(timeout=3)
        self.wait_for('GFD', 'GFD: 1 member: S3')
        self.request('S3')

        # Restarting GFD must reconstruct only the currently healthy membership.
        gfd.terminate()
        gfd.wait(timeout=3)
        self.wait_for('LFD3', 'lost GFD connection')
        before = self.output('LFD3').count('receives heartbeat from S3')
        self.wait_for('LFD3', 'receives heartbeat from S3', before + 3)
        gfd = self.gfd('GFD-restart')
        self.wait_for('GFD-restart', 'GFD: 1 member: S3')
        self.assertNotIn('add replica S1', self.output('GFD-restart'))
        self.assertNotIn('add replica S2', self.output('GFD-restart'))
        servers[1] = self.launch('S1-restart', 'server', '--id', 'S1')
        self.wait_for('GFD-restart', 'LFD1: add replica S1')

        # Losing an LFD also removes its member at GFD.
        lfds[3].terminate()
        lfds[3].wait(timeout=3)
        self.wait_for('GFD-restart', 'LFD3: delete replica S3')
        self.assertIsNone(gfd.poll())
        for name in self.logs:
            self.assertNotIn('Traceback', self.output(name))

    def test_gfd_unavailable_at_start(self):
        lfd = self.launch('LFD1', 'lfd', '--id', 'LFD1', '--freq', '10', '--timeout', '.5')
        self.wait_for('LFD1', 'listening')
        self.launch('S1', 'server', '--id', 'S1')
        self.wait_for('LFD1', 'receives heartbeat from S1', 3)
        self.request('S1')
        self.gfd()
        self.wait_for('GFD', 'GFD: 1 member: S1')
        self.wait_for('GFD', 'GFD receives heartbeat from LFD1', 3)
        self.assertEqual(self.output('GFD').count('LFD1: add replica S1'), 1)
        self.assertIsNone(lfd.poll())

    def test_partial_gfd_message_does_not_block_server(self):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('127.0.0.1', self.ports['GFD']))
            listener.listen()
            listener.settimeout(3)
            self.launch('LFD1', 'lfd', '--id', 'LFD1', '--freq', '10', '--timeout', '.5')
            with listener.accept()[0] as peer:
                peer.settimeout(3)
                with peer.makefile('rb') as stream:
                    self.assertEqual(json.loads(stream.readline()),
                                     dict(type='register_lfd', lfd_id='LFD1'))
                    peer.sendall(b'{"type":"heartbeat","from":"GFD",')
                    self.launch('S1', 'server', '--id', 'S1')
                    self.wait_for('LFD1', 'receives heartbeat from S1', 4)
                    self.assertEqual(json.loads(stream.readline()),
                                     dict(type='add_replica', lfd_id='LFD1', replica_id='S1'))
                    peer.sendall(b'"to":"LFD1","count":7}\n')
                    self.assertEqual(json.loads(stream.readline()),
                                     dict(type='heartbeat_ack', **{'from': 'LFD1', 'to': 'GFD'}, count=7))

    def test_invalid_gfd_peers_are_isolated(self):
        gfd = self.gfd()
        self.wait_for('GFD', 'listening')
        for payload in [b'not-json\n', b'[]\n',
                        b'{"type":"add_replica","lfd_id":"LFD1","replica_id":"S1"}\n',
                        b'{"type":"register_lfd","lfd_id":"LFD1"}\n'
                        b'{"type":"add_replica","lfd_id":"LFD1","replica_id":"S2"}\n']:
            with socket.create_connection(('127.0.0.1', self.ports['GFD']), timeout=2) as sock:
                sock.sendall(payload)
                while sock.recv(4096):
                    pass
            self.assertIsNone(gfd.poll())
        self.assertNotIn('add replica', self.output('GFD'))
        self.launch('LFD2', 'lfd', '--id', 'LFD2', '--freq', '10', '--timeout', '.5')
        self.wait_for('GFD', 'GFD receives heartbeat from LFD2', 2)

if __name__ == '__main__':
    unittest.main()
