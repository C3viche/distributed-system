"""Shared M3 configuration contracts, independent of server implementation."""

import os
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

from dotenv import load_dotenv

CONFIG_PATH = Path(__file__).resolve().parents[1] / "src/distributed_system/config.py"


class M3ConfigTests(unittest.TestCase):
    def read_config(self, values):
        # Isolate each import from the developer's shell and local .env.
        with patch.dict(os.environ, values, clear=True), patch("dotenv.load_dotenv"):
            return runpy.run_path(str(CONFIG_PATH))

    def test_defaults(self):
        config = self.read_config({})
        self.assertEqual(config["REPLICATION_MODE"], "active")
        self.assertEqual(config["PRIMARY_ID"], "S1")
        self.assertEqual(config["CHECKPOINT_FREQ"], 1.0)

    def test_environment_overrides_and_fractional_frequency(self):
        for primary in ("S1", "S2", "S3"):
            with self.subTest(primary=primary):
                config = self.read_config({"PRIMARY_ID": primary, "CHECKPOINT_FREQ": "0.5"})
                self.assertEqual(config["PRIMARY_ID"], primary)
                self.assertEqual(config["CHECKPOINT_FREQ"], 0.5)

    def test_invalid_primary(self):
        for primary in ("", "S4", "LFD1", "s1"):
            with self.subTest(primary=primary), self.assertRaisesRegex(ValueError, "PRIMARY_ID must be one of"):
                _ = self.read_config({"PRIMARY_ID": primary})

    def test_replication_modes(self) -> None:
        for mode in ("active", "passive"):
            with self.subTest(mode=mode):
                self.assertEqual(self.read_config({"REPLICATION_MODE": mode})["REPLICATION_MODE"], mode)

    def test_invalid_replication_mode(self) -> None:
        for mode in ("", "ACTIVE", "Passive", "unknown", " passive "):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "REPLICATION_MODE must be"):
                _ = self.read_config({"REPLICATION_MODE": mode})

    def test_invalid_frequency(self):
        for frequency in ("", "abc", "0", "-1", "nan", "inf", "-inf", "1e309"):
            with self.subTest(frequency=frequency), self.assertRaisesRegex(ValueError, "CHECKPOINT_FREQ must be a finite, positive"):
                _ = self.read_config({"CHECKPOINT_FREQ": frequency})

    def test_dotenv_values_and_environment_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            dotenv_path = Path(directory) / ".env"
            dotenv_path.write_text("REPLICATION_MODE=passive\nPRIMARY_ID=S2\nCHECKPOINT_FREQ=2\n")
            for values, expected in (({}, ("passive", "S2", 2.0)),
                                     ({"REPLICATION_MODE": "active", "PRIMARY_ID": "S3", "CHECKPOINT_FREQ": "4"}, ("active", "S3", 4.0))):
                with self.subTest(values=values):
                    with patch.dict(os.environ, values, clear=True), patch(
                        "dotenv.load_dotenv", side_effect=lambda: load_dotenv(dotenv_path)
                    ):
                        config = runpy.run_path(str(CONFIG_PATH))
                    self.assertEqual((config["REPLICATION_MODE"], config["PRIMARY_ID"], config["CHECKPOINT_FREQ"]), expected)


if __name__ == "__main__":
    unittest.main()
