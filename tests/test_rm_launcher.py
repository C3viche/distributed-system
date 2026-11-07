"""Check RM orchestration with stub commands, without opening Terminal windows."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RMLauncherTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        shutil.copyfile(ROOT / "run_all.sh", self.root / "run_all.sh")
        (self.root / ".env").write_text("")
        bindir = self.root / "bin"
        bindir.mkdir()
        for name, body in {
            "osascript": 'cat >/dev/null\nexit 0',
            "sleep": 'exit 0',
            "lsof": 'exit 1',
            "uv": 'cat >/dev/null\nexit "${WAIT_STATUS:-0}"',
        }.items():
            path = bindir / name
            path.write_text("#!/bin/bash\n" + body + "\n")
            path.chmod(0o755)
        self.env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}"}

    def run_script(self, *args, wait_status="0"):
        return subprocess.run(
            ["/bin/bash", str(self.root / "run_all.sh"), *args],
            env={**self.env, "WAIT_STATUS": wait_status},
            capture_output=True, text=True, timeout=10,
        )

    def test_m4_modes_start_rm_before_gfd(self):
        for mode in ("m4-active", "m4-passive"):
            with self.subTest(mode=mode):
                result = self.run_script("local", mode)
                self.assertEqual(result.returncode, 0, result.stderr)
                launched = [line for line in result.stdout.splitlines()
                            if line.startswith("opened window:")]
                self.assertEqual(launched[:2], ["opened window: RM", "opened window: GFD"])
                self.assertEqual(len(launched), 11)

    def test_m3_keeps_existing_launch_order(self):
        result = self.run_script("local", "m3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("opened window: RM", result.stdout)
        self.assertIn("opened window: GFD", result.stdout)

    def test_infrastructure_and_standalone_rm(self):
        for args, expected in ((["rm"], ["RM"]), (["infrastructure"], ["RM", "GFD"])):
            with self.subTest(args=args):
                result = self.run_script(*args)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.splitlines(),
                                 [f"opened window: {name}" for name in expected])

    def test_failed_rm_readiness_stops_downstream_startup(self):
        for args in (["infrastructure"], ["local", "m4-active"]):
            with self.subTest(args=args):
                result = self.run_script(*args, wait_status="1")
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("opened window: GFD", result.stdout)


if __name__ == "__main__":
    unittest.main()
