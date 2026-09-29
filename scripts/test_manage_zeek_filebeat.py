import gzip
import importlib.util
import json
import os
import tempfile
import unittest
from unittest import mock


MODULE_PATH = os.path.join(os.path.dirname(__file__), "manage_zeek_filebeat.py")
SPEC = importlib.util.spec_from_file_location("manage_zeek_filebeat", MODULE_PATH)
MANAGER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MANAGER)


class ArchiveStageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source_root = os.path.join(self.temp.name, "zeek-logs")
        self.stage_root = os.path.join(self.temp.name, "stage")
        self.source_day = os.path.join(self.source_root, "2026-09-28")
        os.makedirs(self.source_day)

    def run_sync(self, now, acknowledged_stages=None):
        MANAGER.sync_archives_once(self.source_root, self.stage_root, now, acknowledged_stages)

    def test_expired_stage_is_retained_until_filebeat_ack_is_known(self):
        source = os.path.join(self.source_day, "conn.2026-09-28-10.log.gz")
        with gzip.open(source, "wb") as handle:
            handle.write(b'{"uid":"source-evidence"}\n')
        now = os.path.getmtime(source) + 2 * 60 * 60
        os.utime(source, (now - 2 * 60 * 60, now - 2 * 60 * 60))

        stage_dir = os.path.join(self.stage_root, "conn")
        os.makedirs(stage_dir)
        expired_stage = os.path.join(stage_dir, "old.log")
        with open(expired_stage, "wb") as handle:
            handle.write(b"expired copy")
        expired_time = now - MANAGER.ARCHIVE_STAGE_RETENTION_SECONDS - 1
        os.utime(expired_stage, (expired_time, expired_time))

        self.run_sync(now)

        self.assertTrue(os.path.exists(expired_stage), "age alone must not delete unacknowledged input")
        self.assertTrue(os.path.exists(source), "cleanup must preserve source gzip evidence")
        self.assertFalse(os.path.exists(os.path.join(stage_dir, "conn.2026-09-28-10.log")))

        self.run_sync(now + 1, {expired_stage})
        self.assertFalse(os.path.exists(expired_stage), "acknowledged expired stage should be reclaimed")
        self.assertTrue(os.path.exists(source), "stage cleanup must preserve source gzip evidence")

    def test_recent_archive_is_decompressed_and_unchanged_source_is_not_recopied(self):
        now = 1_790_000_000
        source = os.path.join(self.source_day, "dns.2026-09-28-12.log.gz")
        expected = b'{"query":"example.test"}\n'
        with gzip.open(source, "wb") as handle:
            handle.write(expected)
        source_time = now - 30 * 60
        os.utime(source, (source_time, source_time))

        self.run_sync(now)
        destination = os.path.join(self.stage_root, "dns", "dns.2026-09-28-12.log")
        marker = destination + ".source.json"
        with open(destination, "rb") as handle:
            self.assertEqual(handle.read(), expected)
        with open(marker, "r") as handle:
            identity = json.load(handle)
        self.assertEqual(identity["path"], source)
        self.assertTrue(os.path.exists(source))

        original_mtime = os.stat(destination).st_mtime_ns
        self.run_sync(now + 1)
        self.assertEqual(os.stat(destination).st_mtime_ns, original_mtime)

    def test_corrupt_gzip_does_not_leave_partial_stage(self):
        now = 1_790_000_000
        source = os.path.join(self.source_day, "ssl.2026-09-28-12.log.gz")
        with open(source, "wb") as handle:
            handle.write(b"not a gzip stream")
        source_time = now - 30 * 60
        os.utime(source, (source_time, source_time))

        self.run_sync(now)

        destination = os.path.join(self.stage_root, "ssl", "ssl.2026-09-28-12.log")
        self.assertTrue(os.path.exists(source))
        self.assertFalse(os.path.exists(destination))
        self.assertFalse(os.path.exists(destination + ".partial"))

    def test_registry_acknowledgement_requires_a_stable_cursor_at_eof(self):
        now = 1_790_000_000
        data_root = os.path.join(self.temp.name, "data")
        for dataset in MANAGER.CONTEXTS:
            os.makedirs(os.path.join(self.stage_root, dataset), exist_ok=True)
        acked = os.path.join(self.stage_root, "conn", "acked.log")
        partial = os.path.join(self.stage_root, "dns", "partial.log")
        untracked = os.path.join(self.stage_root, "http", "untracked.log")
        for path, content in ((acked, b"one\ntwo\n"), (partial, b"one\ntwo\n"), (untracked, b"one\n")):
            with open(path, "wb") as handle:
                handle.write(content)
            old = now - MANAGER.ARCHIVE_STAGE_RETENTION_SECONDS - 10
            os.utime(path, (old, old))

        def write_registry(dataset, entries, operations=()):
            root = os.path.join(data_root, dataset, MANAGER.REGISTRY_ROOT_NAME)
            os.makedirs(root)
            snapshot = os.path.join(root, "200.json")
            with open(snapshot, "w") as handle:
                json.dump(entries, handle)
            with open(os.path.join(root, "active.dat"), "w") as handle:
                handle.write(snapshot)
            with open(os.path.join(root, "log.json"), "w") as handle:
                for operation, key, state in operations:
                    handle.write(json.dumps({"op": operation, "id": 201}) + "\n")
                    handle.write(json.dumps({"k": key, "v": state}) + "\n")

        write_registry("conn", [{"_key": "conn-acked", "meta": {"source": acked}, "cursor": {"offset": 8}}])
        write_registry("dns", [{"_key": "dns-partial", "meta": {"source": partial}, "cursor": {"offset": 4}}], [
            ("set", "dns-partial", {"meta": {"source": partial}, "cursor": {"offset": 4}}),
        ])
        write_registry("http", [])
        write_registry("ssl", [])

        acknowledged = MANAGER.acknowledged_archive_stages(data_root, self.stage_root, now)
        self.assertEqual(acknowledged, {os.path.realpath(acked)}, "registry snapshot/log did not yield the fully acknowledged cursor")

        MANAGER.cleanup_expired_archive_stages(os.path.join(self.stage_root, "conn"), now, acknowledged)
        MANAGER.cleanup_expired_archive_stages(os.path.join(self.stage_root, "dns"), now, acknowledged)
        MANAGER.cleanup_expired_archive_stages(os.path.join(self.stage_root, "http"), now, acknowledged)
        self.assertFalse(os.path.exists(acked), "fully acknowledged stage is eligible for reclaim")
        self.assertTrue(os.path.exists(partial), "partially acknowledged stage must remain")
        self.assertTrue(os.path.exists(untracked), "untracked stage must remain")

    def test_unreadable_or_incomplete_registry_fails_closed(self):
        now = 1_790_000_000
        data_root = os.path.join(self.temp.name, "data")
        for dataset in MANAGER.CONTEXTS:
            os.makedirs(os.path.join(self.stage_root, dataset), exist_ok=True)
            registry = os.path.join(data_root, dataset, MANAGER.REGISTRY_ROOT_NAME)
            os.makedirs(registry)
            with open(os.path.join(registry, "active.dat"), "w") as handle:
                handle.write(os.path.join(registry, "missing.json"))
            with open(os.path.join(registry, "log.json"), "w") as handle:
                handle.write('{"op":"set","id":1}\n')
        stage = os.path.join(self.stage_root, "conn", "old.log")
        with open(stage, "wb") as handle:
            handle.write(b"data")
        old = now - MANAGER.ARCHIVE_STAGE_RETENTION_SECONDS - 1
        os.utime(stage, (old, old))

        self.assertEqual(MANAGER.acknowledged_archive_stages(data_root, self.stage_root, now), set())
        MANAGER.sync_archives_once(self.source_root, self.stage_root, now, set())
        self.assertTrue(os.path.exists(stage))

    def test_disk_full_during_stage_write_preserves_source_and_removes_partial(self):
        now = 1_790_000_000
        source = os.path.join(self.source_day, "conn.2026-09-28-12.log.gz")
        with gzip.open(source, "wb") as handle:
            handle.write(b'{"uid":"must-remain-recoverable"}\n')
        source_time = now - 30 * 60
        os.utime(source, (source_time, source_time))

        destination = os.path.join(self.stage_root, "conn", "conn.2026-09-28-12.log")
        with mock.patch.object(MANAGER.shutil, "copyfileobj", side_effect=OSError(28, "No space left on device")):
            self.run_sync(now)

        self.assertTrue(os.path.exists(source), "disk-full recovery must retain the original gzip")
        self.assertFalse(os.path.exists(destination), "incomplete expansion must not become visible to Filebeat")
        self.assertFalse(os.path.exists(destination + ".partial"))


if __name__ == "__main__":
    unittest.main()
