"""Primary is the first remaining member in GFD/RM membership order."""

import unittest

from distributed_system.server.election import backups_for, elect_primary


class ElectionTests(unittest.TestCase):
    def test_empty_group_has_no_primary(self) -> None:
        self.assertIsNone(elect_primary([]))
        self.assertEqual(backups_for("S1", []), [])

    def test_first_alive_member_is_primary(self) -> None:
        self.assertEqual(elect_primary(["S1", "S2", "S3"]), "S1")
        self.assertEqual(elect_primary(["S2", "S3"]), "S2")
        self.assertEqual(elect_primary(["S3"]), "S3")

    def test_recovered_primary_is_appended_and_stays_backup(self) -> None:
        members = ["S2", "S3", "S1"]
        self.assertEqual(elect_primary(members), "S2")
        self.assertEqual(backups_for("S2", members), ["S3", "S1"])

    def test_unknown_ids_are_skipped(self) -> None:
        self.assertEqual(elect_primary(["RM", "S3"]), "S3")
        self.assertEqual(backups_for("S3", ["RM", "S3", "S1"]), ["S1"])
