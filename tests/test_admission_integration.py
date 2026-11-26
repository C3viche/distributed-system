"""Run real RM, GFD, LFD, and server processes through admission and restart."""

import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from itertools import groupby
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class AdmissionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.processes = []
        self.logs = {}
        self.env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PRIMARY_ID": "S1"}
        self.ports = {}
        reservations = []
        for name in ("RM", "GFD", "LFD1", "LFD2", "LFD3", "S1", "S2", "S3"):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reservations.append(sock)
            self.ports[name] = sock.getsockname()[1]
            self.env[name + "_HOST"] = "127.0.0.1"
            self.env[name + "_PORT"] = str(self.ports[name])
        for sock in reservations:
            sock.close()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for output in self.logs.values():
            output.close()

    def output(self, name):
        stream = self.logs[name]
        return os.pread(stream.fileno(), os.fstat(stream.fileno()).st_size, 0).decode()

    def wait_for(self, name, text, count=1):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if self.output(name).count(text) >= count:
                return
            time.sleep(0.03)
        self.fail(f"Missing {text!r} in {name}:\n" + "\n".join(
            f"{key}:\n{self.output(key)}" for key in self.logs))

    def start(self, name, module, *args):
        if name not in self.logs:
            self.logs[name] = tempfile.TemporaryFile()
        process = subprocess.Popen([sys.executable, "-m", module, *args], cwd=ROOT,
                                   env=self.env, stdout=self.logs[name], stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    def start_group(self, mode):
        self.env["REPLICATION_MODE"] = mode
        self.start("RM", "distributed_system.rm.rm")
        self.wait_for("RM", "RM listening")
        self.start("GFD", "distributed_system.gfd.gfd", "--heartbeat-freq", "10", "--timeout", "1")
        self.wait_for("RM", "GFD registered")
        for name in ("LFD1", "LFD2"):
            self.start(name, "distributed_system.lfd.lfd", "--id", name,
                       "--heartbeat-freq", "10", "--timeout", "1")
        self.start("S1", "distributed_system.server.server", "--id", "S1",
                   "--checkpoint-freq", "10")
        self.wait_for("S1", "RM confirmed readiness")
        second = self.start("S2", "distributed_system.server.server", "--id", "S2",
                            "--checkpoint-freq", "10")
        self.wait_for("S2", "RM confirmed readiness")
        return second

    def request(self, replica, number):
        with socket.create_connection(("127.0.0.1", self.ports[replica]), timeout=2) as sock:
            sock.sendall((json.dumps({"type": "request", "client_id": "C1",
                                     "request_num": number}) + "\n").encode())
            with sock.makefile("rb") as stream:
                return json.loads(stream.readline())["state"]

    def restart_backup(self, mode):
        second = self.start_group(mode)
        self.assertEqual(self.request("S1", 1), 1)
        if mode == "active":
            self.assertEqual(self.request("S2", 1), 1)
        second.kill()
        second.wait(timeout=3)
        self.wait_for("RM", "RM: 1 member: S1")
        self.assertEqual(self.request("S1", 2), 2)
        self.start("S2", "distributed_system.server.server", "--id", "S2")
        self.wait_for("S2", "RM confirmed readiness", count=2)
        self.wait_for("RM", "RM: S2 synchronized and ready", count=2)
        if mode == "active":
            # Keep traffic quiescent during the snapshot; catch-up is separate work.
            self.assertEqual(self.request("S2", 3), 3)
            self.assertEqual(self.request("S1", 3), 3)
        else:
            self.wait_for("S2", "my_state = 0 -> 2")
            self.assertEqual(self.request("S1", 3), 3)
            self.wait_for("S2", "my_state = 2 -> 3")

    def test_active_restart_with_quiescent_transfer(self):
        self.restart_backup("active")

    def test_passive_backup_restart(self):
        self.restart_backup("passive")

    def test_primary_failure_promotes_backup_and_retargets_clients(self):
        self.start_group("passive")
        self.start("LFD3", "distributed_system.lfd.lfd", "--id", "LFD3",
                   "--heartbeat-freq", "10", "--timeout", "1")
        self.start("S3", "distributed_system.server.server", "--id", "S3")
        self.wait_for("S3", "RM confirmed readiness")
        self.assertEqual(self.request("S1", 1), 1)
        self.wait_for("S2", "my_state = 0 -> 1")
        self.wait_for("S3", "my_state = 0 -> 1")
        # Idle real clients observe GFD's assignment before and after failure.
        for client in ("C1", "C2", "C3"):
            self.start(client, "distributed_system.client", "--id", client, "--interval", "0.2")
            self.wait_for(client, "Received <")
        primary = next(process for process in self.processes
                       if "S1" in process.args)
        primary.kill()
        primary.wait(timeout=3)
        self.wait_for("S2", "promoted to primary")
        self.wait_for("GFD", "GFD: primary is S2")
        self.wait_for("S3", "primary is now S2")
        self.wait_for("S2", "opened checkpoint channel to S3")
        for client in ("C1", "C2", "C3"):
            self.wait_for(client, "assigned primary: S2")
            self.wait_for(client, f"Received <{client}, S2,")
        self.start("S1", "distributed_system.server.server", "--id", "S1")
        self.wait_for("S1", "RM confirmed readiness", count=2)
        self.wait_for("S1", "role=backup")
        self.assertNotIn("role: primary → backup", self.output("S1"))

    def start_third(self):
        self.start("LFD3", "distributed_system.lfd.lfd", "--id", "LFD3",
                   "--heartbeat-freq", "10", "--timeout", "1")
        self.start("S3", "distributed_system.server.server", "--id", "S3",
                   "--checkpoint-freq", "10")
        self.wait_for("S3", "RM confirmed readiness")

    def assert_client_completed(self, client, count):
        numbers = re.findall(r"Received <" + client + r", S[123], (\d+), reply", self.output(client))
        self.assertEqual([int(number) for number in numbers], list(range(1, count + 1)))

    def test_m5_active_fault_sequence(self):
        self.run_m5_scenario("active", ("S2", "S2", "S3"))

    def test_m5_passive_fault_sequence(self):
        self.run_m5_scenario("passive", ("S3", "S3", "S2"))

    @unittest.skipUnless(hasattr(signal, "SIGSTOP"), "requires POSIX SIGSTOP")
    def test_m5_frozen_replica_is_replaced(self):
        self.run_m5_scenario("active", ("S2",), freeze=True)

    def run_m5_scenario(self, mode, faults, *, freeze=False):
        primary = "S3" if mode == "passive" else "S1"
        replica_ids = ("S1", "S2", "S3")
        self.env.update(REPLICATION_MODE=mode, PRIMARY_ID=primary, CHECKPOINT_FREQ="20")

        # A1/P1: bring up the fault detectors before launching any replicas.
        self.start("RM", "distributed_system.rm.rm")
        self.wait_for("RM", "RM listening")
        self.start("GFD", "distributed_system.gfd.gfd", "--heartbeat-freq", "10", "--timeout", "1")
        self.wait_for("RM", "GFD registered")
        for index in range(1, 4):
            lfd = f"LFD{index}"
            self.start(lfd, "distributed_system.lfd.lfd", "--id", lfd,
                       "--heartbeat-freq", "10", "--timeout", "0.5")
            self.wait_for("GFD", f"{lfd} registered with GFD")
        self.assertNotIn("synchronized and ready", self.output("RM"))

        # A2/P2: get three ready replicas, including the configured passive primary.
        replicas = {}
        for replica in (primary, *(name for name in replica_ids if name != primary)):
            replicas[replica] = self.start(replica, "distributed_system.server.server", "--id", replica)
            self.wait_for(replica, "RM confirmed readiness")
        if mode == "passive":
            self.wait_for("GFD", f"GFD: primary is {primary}")

        # A3/P3: keep the same three client processes alive throughout the faults.
        request_count = 100
        clients = {
            client: self.start(client, "distributed_system.client", "--id", client,
                               "--interval", "0.06", "--count", str(request_count))
            for client in ("C1", "C2", "C3")
        }
        for client in clients:
            for replica in replicas:
                self.wait_for(client, f"connected to {replica} at")
            self.wait_for(client, "Received <", count=5)

        # A4–A7/P4–P7: first failure/recovery, same replica again, then a different one.
        generations = dict.fromkeys(replicas, 1)
        for phase, replica in enumerate(faults, start=1):
            with self.subTest(fault=phase, replica=replica):
                generation = generations[replica]
                victim = replicas[replica]
                losses = self.output("RM").count("RM: 2 members:")
                if freeze:
                    victim.send_signal(signal.SIGSTOP)
                else:
                    victim.kill()
                    self.assertEqual(victim.wait(timeout=3), -signal.SIGKILL)
                self.wait_for(f"LFD{replica[1:]}", f"{replica} has died", count=generation)
                self.wait_for("RM", "RM: 2 members:", count=losses + 1)
                if freeze:
                    self.assertIsNone(victim.poll(), "LFD must detect a live but frozen process")
                self.wait_for_client_progress(clients)

                connected = f"connected to {replica} at"
                connections = {client: self.output(client).count(connected) for client in clients}
                replicas[replica] = self.relaunch_for_m5_test(replica, victim)
                generations[replica] += 1
                self.wait_for(replica, "RM confirmed readiness", count=generations[replica])
                for client in clients:
                    self.wait_for(client, connected, count=connections[client] + 1)
                self.wait_for_client_progress(clients)

        for client, process in clients.items():
            self.assertEqual(process.wait(timeout=20), 0, self.output(client))
            self.assert_client_completed(client, request_count)
            output = self.output(client)
            gap = re.search(r"longest reply gap: (\d+\.\d+) s", output)
            self.assertIsNotNone(gap, output)
            # Allow heartbeat and failover detection plus scheduling slack on localhost.
            limit = 2.5 if mode == "passive" else 1.5
            self.assertLess(float(gap[1]), limit, f"{client} longest reply gap was {gap[1]} s")
            if mode == "passive":
                sources = re.findall(r"Received <" + client + r", (S[123]), \d+, reply", output)
                self.assertEqual([replica for replica, _ in groupby(sources)], [primary, "S1"])

        final_state = request_count * len(clients)
        for replica in replicas:
            if mode == "passive" and replica != "S1":
                self.wait_for(replica, f" -> {final_state}")
                states = re.findall(r"my_state = \d+ -> (\d+)", self.output(replica))
            else:
                self.wait_for(replica, f"my_state = {final_state} after processing")
                states = re.findall(r"my_state = (\d+) after processing", self.output(replica))
            self.assertEqual(int(states[-1]), final_state, f"final state on {replica}")
        if mode == "passive":
            self.assertEqual(re.findall(r"GFD: primary is (S[123])", self.output("GFD")), [primary, "S1"])
            for replica in replicas:
                self.assertEqual(self.output(replica).count("promoted to primary"), int(replica == "S1"))

    def wait_for_client_progress(self, clients):
        counts = {client: self.output(client).count("Received <") for client in clients}
        for client, process in clients.items():
            self.wait_for(client, "Received <", count=counts[client] + 2)
            self.assertIsNone(process.poll(), f"{client} exited during fault injection")

    def relaunch_for_m5_test(self, replica, victim):
        """Stand in for RM-driven relaunch while that feature is in development."""
        if victim.poll() is None:
            victim.kill()
            self.assertEqual(victim.wait(timeout=3), -signal.SIGKILL)
        return self.start(replica, "distributed_system.server.server", "--id", replica)

    def test_active_recovery_with_three_clients_under_live_traffic(self):
        self.active_recovery_with_live_traffic("S2")

    def test_active_initial_donor_recovers_while_survivors_serve(self):
        self.active_recovery_with_live_traffic("S1")

    def active_recovery_with_live_traffic(self, crashed):
        second = self.start_group("active")
        self.start_third()
        for replica in ("S2", "S3"):
            self.wait_for("S1", f"opened checkpoint channel to {replica}")
        clients = {client: self.start(client, "distributed_system.client", "--id", client,
                                     "--interval", "0.05", "--count", "100")
                   for client in ("C1", "C2", "C3")}
        for client in clients:
            self.wait_for(client, "Received <", count=5)
        victim = second if crashed == "S2" else next(
            p for p in self.processes if "S1" in p.args)
        victim.kill()
        victim.wait(timeout=3)
        survivors = "S1, S3" if crashed == "S2" else "S2, S3"
        self.wait_for("RM", f"RM: 2 members: {survivors}")
        before = {client: self.output(client).count("Received <") for client in clients}
        for client in clients:
            self.wait_for(client, "Received <", count=before[client] + 3)
        self.start(crashed, "distributed_system.server.server", "--id", crashed)
        self.wait_for(crashed, "RM confirmed readiness", count=2)
        donor = "S1" if crashed == "S2" else "S2"
        self.wait_for(donor, f"opened checkpoint channel to {crashed}", count=2)
        for client, process in clients.items():
            self.assertEqual(process.wait(timeout=20), 0)
            self.assert_client_completed(client, 100)
        # All 300 distinct requests must exist on every replica. Repeating an old
        # request cannot change the state, and one common new request adds one.
        for replica in ("S1", "S2", "S3"):
            self.wait_for(replica, "my_state = 300 after processing")
        for replica in ("S1", "S2", "S3"):
            self.request(replica, 1)
            self.assertEqual(self.request(replica, 101), 301)
        self.assertIn("Discarded duplicate", self.output("C1"))

    def test_passive_failover_preserves_all_acknowledged_client_requests(self):
        self.start_group("passive")
        self.start_third()
        clients = {client: self.start(client, "distributed_system.client", "--id", client,
                                     "--interval", "0.03", "--count", "35")
                   for client in ("C1", "C2", "C3")}
        for client in clients:
            self.wait_for(client, "Received <", count=4)
        primary = next(p for p in self.processes if "S1" in p.args)
        primary.kill()
        primary.wait(timeout=3)
        self.wait_for("S2", "promoted to primary")
        self.wait_for("S2", "opened checkpoint channel to S3")
        for client, process in clients.items():
            self.assertEqual(process.wait(timeout=25), 0)
            self.assert_client_completed(client, 35)
        self.assertEqual(self.request("S2", 36), 106)
        self.wait_for("S3", "-> 106")
        self.start("S1", "distributed_system.server.server", "--id", "S1")
        self.wait_for("S1", "RM confirmed readiness", count=2)
        self.wait_for("S1", "my_state = 0 -> 106")

    def test_checkpointed_request_with_lost_reply_is_not_applied_after_failover(self):
        self.start_group("passive")
        self.start_third()
        with socket.create_connection(("127.0.0.1", self.ports["S1"]), timeout=2) as sock:
            sock.sendall((json.dumps({"type": "request", "client_id": "C1",
                                     "request_num": 1}) + "\n").encode())
            self.wait_for("S2", "my_state = 0 -> 1")
            self.wait_for("S3", "my_state = 0 -> 1")
            # Deliberately never read the reply before killing the primary.
            primary = next(p for p in self.processes if "S1" in p.args)
            primary.kill()
            primary.wait(timeout=3)
        self.wait_for("S2", "promoted to primary")
        self.assertEqual(self.request("S2", 1), 1)
        self.assertEqual(self.request("S2", 2), 2)

    def test_passive_backup_recovers_while_three_clients_continue(self):
        second = self.start_group("passive")
        self.start_third()
        clients = {client: self.start(client, "distributed_system.client", "--id", client,
                                     "--interval", "0.05", "--count", "45")
                   for client in ("C1", "C2", "C3")}
        for client in clients:
            self.wait_for(client, "Received <", count=4)
        second.kill()
        second.wait(timeout=3)
        self.wait_for("RM", "RM: 2 members: S1, S3")
        before = {client: self.output(client).count("Received <") for client in clients}
        for client in clients:
            self.wait_for(client, "Received <", count=before[client] + 2)
        self.start("S2", "distributed_system.server.server", "--id", "S2")
        self.wait_for("S2", "RM confirmed readiness", count=2)
        for client, process in clients.items():
            self.assertEqual(process.wait(timeout=25), 0)
            self.assert_client_completed(client, 45)
        self.assertEqual(self.request("S1", 46), 136)
        for replica in ("S2", "S3"):
            self.wait_for(replica, "-> 136")

    def test_backups_start_before_primary_without_displacing_it(self):
        self.env["REPLICATION_MODE"] = "passive"
        self.start("RM", "distributed_system.rm.rm")
        self.wait_for("RM", "RM listening")
        self.start("GFD", "distributed_system.gfd.gfd", "--heartbeat-freq", "10", "--timeout", "3")
        self.wait_for("RM", "GFD registered")
        for name in ("LFD1", "LFD2", "LFD3"):
            self.start(name, "distributed_system.lfd.lfd", "--id", name,
                       "--heartbeat-freq", "10", "--timeout", "3")
        self.start("S2", "distributed_system.server.server", "--id", "S2")
        self.wait_for("RM", "RM: 1 member: S2")
        self.start("S3", "distributed_system.server.server", "--id", "S3")
        self.wait_for("RM", "RM: 2 members: S2, S3")
        for replica in ("S2", "S3"):
            self.assertNotIn("RM confirmed readiness", self.output(replica))
        self.start("S1", "distributed_system.server.server", "--id", "S1",
                   "--checkpoint-freq", "10")
        for replica in ("S1", "S2", "S3"):
            self.wait_for(replica, "RM confirmed readiness")
        # A GFD client snapshot verifies the selected primary after all are ready.
        with socket.create_connection(("127.0.0.1", self.ports["GFD"]), timeout=3) as sock:
            sock.sendall(b'{"type":"register_client","client_id":"startup-check"}\n')
            with sock.makefile("rb") as stream:
                snapshot = json.loads(stream.readline())
        self.assertEqual(snapshot["primary_id"], "S1")
        self.assertEqual(set(snapshot["members"]), {"S1", "S2", "S3"})
        self.assertEqual(self.request("S1", 1), 1)
        for replica in ("S2", "S3"):
            self.wait_for(replica, "my_state = 0 -> 1")
            self.assertNotIn("promoted to primary", self.output(replica))
        self.assertNotIn("role: primary → backup", self.output("S1"))
