"""LFD auto-relaunch: relaunch_replica from the GFD restarts a dead server."""

# Tests attach sockets to the selector directly and replace the launcher.
# pyright: reportPrivateUsage=false, reportAny=false

import io
import os
import selectors
import signal
import socket
import subprocess
import time
import unittest
from unittest.mock import MagicMock, patch

from distributed_system.common import BufferedJsonConnection, recv_json
from distributed_system.lfd.lfd import (
    LocalFaultDetector,
    ServerSession,
    open_terminal_window,
)


def relaunch(attempt: object = 1, replica_id: object = "S2") -> dict[str, object]:
    return {"type": "relaunch_replica", "replica_id": replica_id, "attempt": attempt}


class FakeLauncherProcess:
    """Stands in for the terminal launcher process (osascript)."""

    def __init__(self, exit_code: int | None, stderr: bytes = b"") -> None:
        self.exit_code: int | None = exit_code
        self.stderr: io.BytesIO = io.BytesIO(stderr)

    def poll(self) -> int | None:
        return self.exit_code


class LFDRelaunchTests(unittest.TestCase):
    def make_lfd(self, launcher: MagicMock | None = None, cooldown: float = 5.0) -> LocalFaultDetector:
        self.launcher: MagicMock = launcher or MagicMock(return_value=FakeLauncherProcess(0))
        lfd = LocalFaultDetector(
            "LFD2", launcher=self.launcher, workdir="/repo", relaunch_cooldown=cooldown
        )
        self.addCleanup(lfd._selector.close)
        local, self.gfd_peer = socket.socketpair()
        self.addCleanup(local.close)
        self.addCleanup(self.gfd_peer.close)
        self.gfd_peer.settimeout(1.0)
        lfd._gfd.sock = local
        lfd._gfd.connection = BufferedJsonConnection(local)
        lfd._gfd.connecting = False
        lfd._connections[local] = lfd._gfd
        _ = lfd._selector.register(local, selectors.EVENT_READ, "gfd")
        return lfd

    def register_server(self, lfd: LocalFaultDetector, pid: object = 4242) -> socket.socket:
        local, remote = socket.socketpair()
        self.addCleanup(remote.close)
        self.addCleanup(local.close)
        lfd._connections[local] = ServerSession(local, time.monotonic() + 2.0)
        _ = lfd._selector.register(local, selectors.EVENT_READ, "server")
        message: dict[str, object] = {"type": "registration", "replica_id": "S2"}
        if pid is not None:
            message["pid"] = pid
        lfd._message(local, message)
        return local

    def sent_to_gfd(self, lfd: LocalFaultDetector) -> dict[str, object] | None:
        assert lfd._gfd.connection is not None
        lfd._gfd.connection.flush()
        return recv_json(self.gfd_peer)

    def test_registration_remembers_pid_and_ignores_bad_values(self) -> None:
        lfd = self.make_lfd()
        _ = self.register_server(lfd, pid=4242)
        self.assertEqual(lfd._server_pid, 4242)
        for bad in (None, True, "4242", 0, 1, -5):
            with self.subTest(pid=bad):
                lfd = self.make_lfd()
                _ = self.register_server(lfd, pid=bad)
                self.assertIsNone(lfd._server_pid)

    def test_dead_server_is_killed_then_relaunched_in_new_window(self) -> None:
        lfd = self.make_lfd()
        server = self.register_server(lfd, pid=4242)
        lfd._drop(server)
        with patch("distributed_system.lfd.lfd.os.kill") as kill:
            lfd._handle_gfd(relaunch())
        kill.assert_called_once_with(4242, signal.SIGKILL)
        self.launcher.assert_called_once_with("uv run server --id S2", "/repo")
        self.assertIsNone(lfd._server_pid)

    def test_missing_pid_still_relaunches(self) -> None:
        lfd = self.make_lfd()
        lfd._handle_gfd(relaunch())
        self.launcher.assert_called_once_with("uv run server --id S2", "/repo")

    def test_already_dead_pid_is_not_an_error(self) -> None:
        lfd = self.make_lfd()
        lfd._drop(self.register_server(lfd, pid=4242))
        with patch("distributed_system.lfd.lfd.os.kill", side_effect=ProcessLookupError):
            lfd._handle_gfd(relaunch())
        self.launcher.assert_called_once()

    def test_never_kills_pid_one_or_itself(self) -> None:
        lfd = self.make_lfd()
        lfd._server_pid = os.getpid()
        with patch("distributed_system.lfd.lfd.os.kill") as kill:
            lfd._kill_leftover()
        kill.assert_not_called()

    def test_ignored_while_replica_is_up(self) -> None:
        lfd = self.make_lfd()
        _ = self.register_server(lfd)
        with patch("distributed_system.lfd.lfd.os.kill") as kill:
            lfd._handle_gfd(relaunch())
        kill.assert_not_called()
        self.launcher.assert_not_called()

    def test_ignored_while_a_connection_is_still_registering(self) -> None:
        lfd = self.make_lfd()
        local, remote = socket.socketpair()
        self.addCleanup(remote.close)
        self.addCleanup(local.close)
        lfd._connections[local] = ServerSession(local, time.monotonic() + 2.0)
        _ = lfd._selector.register(local, selectors.EVENT_READ, "server")
        lfd._handle_gfd(relaunch())
        self.launcher.assert_not_called()

    def test_duplicate_request_during_cooldown_starts_one_server(self) -> None:
        lfd = self.make_lfd(cooldown=5.0)
        lfd._handle_gfd(relaunch(attempt=1))
        lfd._handle_gfd(relaunch(attempt=2))
        self.launcher.assert_called_once()
        # After the cooldown the RM's retry is honored.
        assert lfd._relaunch_at is not None
        lfd._relaunch_at -= 10.0
        lfd._handle_gfd(relaunch(attempt=2))
        self.assertEqual(self.launcher.call_count, 2)

    def test_new_registration_clears_cooldown_for_next_death(self) -> None:
        lfd = self.make_lfd(cooldown=60.0)
        lfd._handle_gfd(relaunch())
        server = self.register_server(lfd, pid=5000)
        lfd._drop(server)
        with patch("distributed_system.lfd.lfd.os.kill") as kill:
            lfd._handle_gfd(relaunch())
        kill.assert_called_once_with(5000, signal.SIGKILL)
        self.assertEqual(self.launcher.call_count, 2)

    def test_invalid_requests_are_ignored_and_keep_gfd_connection(self) -> None:
        lfd = self.make_lfd()
        for message in (
            relaunch(replica_id="S3"),
            relaunch(replica_id=None),
            relaunch(attempt=0),
            relaunch(attempt=True),
            relaunch(attempt="1"),
        ):
            with self.subTest(message=message):
                lfd._handle_gfd(message)
        self.launcher.assert_not_called()
        assert lfd._gfd.sock is not None
        self.assertIn(lfd._gfd.sock, lfd._connections)

    def test_heartbeats_still_validated_strictly(self) -> None:
        lfd = self.make_lfd()
        with self.assertRaises(ValueError):
            lfd._handle_gfd({"type": "membership", "members": []})

    def test_launcher_oserror_reports_relaunch_failed_to_gfd(self) -> None:
        lfd = self.make_lfd(launcher=MagicMock(side_effect=FileNotFoundError("osascript")))
        lfd._handle_gfd(relaunch())
        message = self.sent_to_gfd(lfd)
        assert message is not None
        self.assertEqual(message["type"], "relaunch_failed")
        self.assertEqual(message["replica_id"], "S2")
        self.assertIn("osascript", str(message["reason"]))
        self.assertIsNone(lfd._relaunch_at)

    def test_failed_launch_allows_the_rms_next_attempt_immediately(self) -> None:
        launcher = MagicMock(side_effect=[OSError("boom"), FakeLauncherProcess(0)])
        lfd = self.make_lfd(launcher=launcher)
        lfd._handle_gfd(relaunch(attempt=1))
        _ = self.sent_to_gfd(lfd)
        lfd._handle_gfd(relaunch(attempt=2))
        self.assertEqual(launcher.call_count, 2)

    def test_launcher_exiting_nonzero_reports_failure_without_blocking(self) -> None:
        proc = FakeLauncherProcess(None)
        lfd = self.make_lfd(launcher=MagicMock(return_value=proc))
        lfd._handle_gfd(relaunch())
        lfd._poll_relaunch()  # still running: nothing reported yet
        self.assertIsNotNone(lfd._relaunch_proc)
        proc.exit_code = 1
        proc.stderr = io.BytesIO(b"Terminal got an error")
        lfd._poll_relaunch()
        message = self.sent_to_gfd(lfd)
        assert message is not None
        self.assertEqual(message["type"], "relaunch_failed")
        self.assertIn("Terminal got an error", str(message["reason"]))
        self.assertIsNone(lfd._relaunch_proc)

    def test_launcher_exiting_cleanly_reports_nothing(self) -> None:
        lfd = self.make_lfd()
        lfd._handle_gfd(relaunch())
        lfd._poll_relaunch()
        self.assertIsNone(lfd._relaunch_proc)
        assert lfd._gfd.connection is not None
        self.assertFalse(lfd._gfd.connection.has_pending_output)

    def test_wait_timeout_wakes_to_poll_a_running_launcher(self) -> None:
        lfd = self.make_lfd(launcher=MagicMock(return_value=FakeLauncherProcess(None)))
        lfd._handle_gfd(relaunch())
        timeout = lfd._wait_timeout()
        assert timeout is not None
        self.assertLessEqual(timeout, 0.2)

    def test_custom_server_command_substitutes_replica(self) -> None:
        lfd = LocalFaultDetector(
            "LFD2", launcher=MagicMock(return_value=FakeLauncherProcess(0)),
            server_command="uv run server --id {replica_id} --checkpoint-freq 2",
            workdir="/repo",
        )
        self.addCleanup(lfd._selector.close)
        lfd._handle_gfd(relaunch())
        cast_launcher = lfd._launcher
        assert isinstance(cast_launcher, MagicMock)
        cast_launcher.assert_called_once_with(
            "uv run server --id S2 --checkpoint-freq 2", "/repo"
        )

    def test_frozen_server_is_killed_and_replaced(self) -> None:
        """A SIGSTOPped server never answers heartbeats but is still running."""
        frozen = subprocess.Popen(["sleep", "60"])
        self.addCleanup(frozen.kill)
        os.kill(frozen.pid, signal.SIGSTOP)
        lfd = self.make_lfd()
        server = self.register_server(lfd, pid=frozen.pid)
        lfd._drop(server)  # heartbeat timeout drops it
        lfd._handle_gfd(relaunch())
        self.assertEqual(frozen.wait(timeout=5), -signal.SIGKILL)
        self.launcher.assert_called_once_with("uv run server --id S2", "/repo")


class OpenTerminalWindowTests(unittest.TestCase):
    def test_macos_opens_terminal_with_escaped_command(self) -> None:
        with (
            patch("distributed_system.lfd.lfd.sys.platform", "darwin"),
            patch("distributed_system.lfd.lfd.subprocess.Popen") as popen,
        ):
            _ = open_terminal_window('uv run server --id S2', '/path with "quote"/repo')
        args = popen.call_args.args[0]
        self.assertEqual(args[0], "osascript")
        script = args[2]
        self.assertIn('tell application "Terminal" to do script', script)
        self.assertIn("uv run server --id S2", script)
        self.assertIn('\\"quote\\"', script)

    def test_fallback_runs_detached_without_a_terminal(self) -> None:
        with (
            patch("distributed_system.lfd.lfd.sys.platform", "linux"),
            patch("distributed_system.lfd.lfd.shutil.which", return_value=None),
            patch("distributed_system.lfd.lfd.subprocess.Popen") as popen,
        ):
            _ = open_terminal_window("uv run server --id S2", "/repo")
        self.assertEqual(popen.call_args.args[0][:2], ["bash", "-lc"])
        self.assertTrue(popen.call_args.kwargs["start_new_session"])


if __name__ == "__main__":
    unittest.main()
