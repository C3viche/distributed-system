"""Check config consumers in fresh processes, isolated from local .env files."""

import os
import subprocess
import sys
import unittest


CONSUMER_CHECK = '''
import os
import socket
import sys
from unittest.mock import patch

with patch("dotenv.load_dotenv"):
    from distributed_system.client import _parse_args as client_args
    from distributed_system.client.client import Client
    from distributed_system.common import BufferedJsonConnection, send_json
    from distributed_system.server.server import Server, _parse_args as server_args

expected_mode = os.environ.get("REPLICATION_MODE", "active")
expected_primary = os.environ.get("PRIMARY_ID", "S1")
client = Client("C1")
servers = [Server(replica_id) for replica_id in ("S1", "S2", "S3")]
try:
    assert client.replication_mode == expected_mode
    assert client.primary_id == expected_primary
    for server in servers:
        assert server.replication_mode == expected_mode
        assert server.primary_id == expected_primary
        expected_role = "active" if expected_mode == "active" else (
            "primary" if server.replica_id == expected_primary else "backup")
        assert server.role == expected_role
    with patch("sys.argv", ["client", "--id", "C1"]):
        assert client_args().replication_mode == expected_mode
    with patch("sys.argv", ["server", "--id", "S1"]):
        args = server_args()
        assert args.replication_mode == expected_mode
        assert args.primary == expected_primary
        assert args.checkpoint_freq == float(os.environ.get("CHECKPOINT_FREQ", "1"))

    override = "passive" if expected_mode == "active" else "active"
    with patch("sys.argv", ["client", "--id", "C1", "--replication-mode", override]):
        assert client_args().replication_mode == override
    with patch("sys.argv", ["server", "--id", "S1", "--replication-mode", override,
                            "--primary", "S3", "--checkpoint-freq", "0.5"]):
        args = server_args()
        assert args.replication_mode == override
        assert args.primary == "S3"
        assert args.checkpoint_freq == 0.5

    receiver, publisher = socket.socketpair()
    try:
        client._gfd = BufferedJsonConnection(receiver)
        with patch("distributed_system.client.client.log"):
            send_json(publisher, {"type": "membership", "members": ["S1", "S2", "S3"], "member_count": 3})
            client._read_gfd()
            assert client.primary_id == expected_primary
            send_json(publisher, {"type": "membership", "members": ["S1", "S2", "S3"],
                                  "member_count": 3, "primary_id": "S3"})
            client._read_gfd()
            assert client.primary_id == "S3"
    finally:
        receiver.close()
        publisher.close()
finally:
    client.conns.close()
    for server in servers:
        server.sel.close()
'''


class ReplicationConfigIntegrationTests(unittest.TestCase):
    def test_shared_defaults_and_cli_overrides(self) -> None:
        for mode in (None, "active", "passive"):
            with self.subTest(mode=mode):
                env = {key: value for key, value in os.environ.items()
                       if key not in {"REPLICATION_MODE", "PRIMARY_ID", "CHECKPOINT_FREQ"}}
                if mode is not None:
                    env.update(REPLICATION_MODE=mode, PRIMARY_ID="S2", CHECKPOINT_FREQ="2")
                result = subprocess.run(
                    [sys.executable, "-c", CONSUMER_CHECK], env=env,
                    capture_output=True, text=True, check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_removed_underscore_flags_and_invalid_frequency(self) -> None:
        for command, arguments in (
            ("client", ["--replication_mode", "passive"]),
            ("server", ["--replication_mode", "passive"]),
            ("server", ["--checkpoint_freq", "2"]),
            *(("server", ["--checkpoint-freq", value]) for value in ("0", "-1", "nan", "inf")),
        ):
            with self.subTest(command=command, arguments=arguments):
                code = (
                    "from unittest.mock import patch\n"
                    "with patch('dotenv.load_dotenv'):\n"
                    f"    from distributed_system.{command}"
                    + (".server" if command == "server" else "")
                    + " import _parse_args\n"
                    "_parse_args()\n"
                )
                env = {key: value for key, value in os.environ.items()
                       if key not in {"REPLICATION_MODE", "PRIMARY_ID", "CHECKPOINT_FREQ"}}
                result = subprocess.run(
                    [sys.executable, "-c", code, "--id", "S1" if command == "server" else "C1", *arguments],
                    env=env, capture_output=True, text=True, check=False,
                )
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("error:", result.stderr)
