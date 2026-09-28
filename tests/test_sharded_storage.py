import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine import dataset_tools
from engine.generic_job import GenericDB, _legacy_migrate_to_shards


class ShardedStorageTests(unittest.TestCase):
    def _spec(self):
        return {
            "name": "ad_daily",
            "source": "insights",
            "fields": ["ad_id", "spend", "actions"],
            "account_ids": [],
            "row_limit": 10000000,
            "page_limit": 100000,
            "time_range": {"mode": "maximum", "date_preset": "maximum"},
            "breakdowns": [],
            "level": "ad",
            "time_increment": "1",
            "filtering": [],
            "limit": 200,
        }

    def test_legacy_daily_rows_migrate_out_of_checkpoint(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        db = GenericDB(path)
        spec = self._spec()
        db.ensure_dataset(spec)
        rows = [
            {"account_id": "1", "ad_id": "a", "date_start": "2026-01-01", "date_stop": "2026-01-01", "spend": "10"},
            {"account_id": "1", "ad_id": "a", "date_start": "2026-01-02", "date_stop": "2026-01-02", "spend": "20"},
            {"account_id": "2", "ad_id": "b", "date_start": "2026-01-01", "date_stop": "2026-01-01", "spend": "30"},
        ]
        self.assertEqual(db.insert_rows("ad_daily", rows), 3)
        uploaded = []

        def fake_gateway(path, method="GET", data=None, mime="application/octet-stream", timeout=120):
            if method == "POST":
                uploaded.append((path, len(data or b"")))
                return b""
            raise AssertionError((path, method))

        with patch("engine.generic_job.gateway_raw", side_effect=fake_gateway):
            _legacy_migrate_to_shards("00000000-0000-0000-0000-000000000001", db, {"datasets": [spec]})

        self.assertEqual(db.db.execute("SELECT COUNT(*) FROM dataset_rows WHERE dataset='ad_daily'").fetchone()[0], 0)
        self.assertEqual(db.dataset_count("ad_daily"), 3)
        self.assertGreater(db.shard_count("ad_daily"), 0)
        self.assertEqual(db.get_meta("storage_version"), 2)
        self.assertTrue(any("/shard?key=" in p for p, _ in uploaded))
        self.assertTrue(any("/checkpoint" in p for p, _ in uploaded))
        db.close()
        os.remove(path)

    def test_query_and_aggregate_stream_r2_shards(self):
        td = tempfile.mkdtemp()
        try:
            meta_path = os.path.join(td, "parent.sqlite3")
            db = GenericDB(meta_path)
            spec = self._spec()
            db.ensure_dataset(spec)
            db.db.execute("UPDATE datasets SET row_count=3,status='done' WHERE name='ad_daily'")
            db.db.execute(
                "INSERT INTO shards(dataset,shard_key,scope_id,account_id,row_count,date_start,date_stop,raw_bytes,gzip_bytes,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                ("ad_daily", "shards/job/ad_daily/1/part.jsonl.gz", "s", "1", 3, "2026-01-01", "2026-01-03", 1, 1, "now"),
            )
            db.db.commit(); db.close()

            rows = [
                {"account_id": "1", "ad_id": "a", "spend": "10", "actions": [{"action_type": "lead", "value": "1"}]},
                {"account_id": "1", "ad_id": "a", "spend": "20", "actions": [{"action_type": "lead", "value": "2"}]},
                {"account_id": "1", "ad_id": "b", "spend": "30", "actions": []},
            ]
            packed = gzip.compress(("\n".join(json.dumps(x) for x in rows) + "\n").encode())

            def fake_load(_):
                out = os.path.join(td, "copy.sqlite3")
                shutil.copy2(meta_path, out)
                return out

            with patch("engine.dataset_tools._load_parent", side_effect=fake_load), patch("engine.dataset_tools.gateway_raw", return_value=packed):
                q = dataset_tools.query_dataset("job", {"dataset": "ad_daily", "fields": ["ad_id", "spend"], "limit": 2})
                self.assertEqual(q["dataset_row_count"], 3)
                self.assertEqual(q["returned"], 2)

            with patch("engine.dataset_tools._load_parent", side_effect=fake_load), patch("engine.dataset_tools.gateway_raw", return_value=packed):
                a = dataset_tools.aggregate_dataset(
                    "job",
                    {
                        "dataset": "ad_daily",
                        "group_by": ["ad_id"],
                        "metrics": [
                            {"name": "spend", "op": "sum", "field": "spend"},
                            {"name": "leads", "op": "action_sum", "field": "actions", "action_type": "lead"},
                            {"name": "cpl", "op": "ratio", "numerator_metric": "spend", "denominator_metric": "leads"},
                        ],
                        "sort_by": "spend",
                        "descending": True,
                        "limit": 10,
                    },
                )
                amap = {x["ad_id"]: x for x in a["groups"]}
                self.assertEqual(amap["a"]["spend"], 30.0)
                self.assertEqual(amap["a"]["leads"], 3.0)
                self.assertEqual(amap["a"]["cpl"], 10.0)
                self.assertEqual(a["matched_rows_scanned"], 3)
        finally:
            shutil.rmtree(td, ignore_errors=True)

    def test_virtual_error_and_coverage_diagnostics(self):
        td = tempfile.mkdtemp()
        try:
            meta_path = os.path.join(td, "parent.sqlite3")
            db = GenericDB(meta_path)
            account_spec = {
                "name": "accounts", "source": "accounts", "fields": ["id", "name"],
                "account_ids": [], "row_limit": 1000, "page_limit": 10,
                "time_range": {"mode": "maximum", "date_preset": "maximum"},
                "breakdowns": [], "level": "account", "time_increment": None,
                "filtering": [], "limit": 100,
            }
            daily_spec = self._spec()
            db.ensure_dataset(account_spec)
            db.ensure_dataset(daily_spec)
            db.insert_rows("accounts", [{"id": "act_123", "account_id": "123", "name": "AA Test"}])
            db.set_progress("ad_daily", "123:2026-01-01:2026-03-31", None, "failed")
            db.error("ad_daily", "123:2026-01-01:2026-03-31", RuntimeError("Meta temporary error"))
            db.close()

            def fake_load(_):
                out = os.path.join(td, "copy.sqlite3")
                shutil.copy2(meta_path, out)
                return out

            with patch("engine.dataset_tools._load_parent", side_effect=fake_load):
                e = dataset_tools.query_dataset("job", {"dataset": "__errors__", "limit": 50})
            self.assertEqual(e["summary"]["total_errors"], 1)
            self.assertEqual(e["rows"][0]["account_id"], "123")
            self.assertEqual(e["rows"][0]["account_name"], "AA Test")
            self.assertEqual(e["rows"][0]["date_since"], "2026-01-01")

            with patch("engine.dataset_tools._load_parent", side_effect=fake_load):
                c = dataset_tools.query_dataset("job", {"dataset": "__coverage__", "filters": [{"field": "status", "op": "eq", "value": "failed"}], "limit": 50})
            self.assertEqual(c["matched_row_count"], 1)
            self.assertEqual(c["rows"][0]["error_count"], 1)
            self.assertEqual(c["rows"][0]["account_name"], "AA Test")
        finally:
            shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
