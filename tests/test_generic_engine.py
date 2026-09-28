import json
import os
import tempfile
import unittest
from unittest.mock import patch

from engine.generic_meta import validate_plan, validate_read_spec, quick_read
from engine.generic_job import GenericDB
from engine.meta import MetaClient


class GenericEngineTests(unittest.TestCase):
    def test_dynamic_plan_accepts_arbitrary_safe_metrics_and_nested_fields(self):
        plan = validate_plan({
            "original_request": "analyze what I asked for",
            "account_scope": {"mode": "all_accessible"},
            "time_range": {"mode": "maximum"},
            "datasets": [
                {"name": "ads", "source": "ads", "fields": ["id", "name", "creative{id,effective_object_story_id}"]},
                {"name": "daily", "source": "insights", "level": "ad", "fields": ["ad_id", "spend", "purchase_roas", "actions"], "time_increment": 1},
            ],
        })
        self.assertEqual(plan["time_range"]["date_preset"], "maximum")
        self.assertIn("purchase_roas", plan["datasets"][1]["fields"])
        self.assertIn("creative{id,effective_object_story_id}", plan["datasets"][0]["fields"])

    def test_object_ids_can_come_from_prior_dataset(self):
        x = validate_read_spec({
            "name": "posts",
            "source": "objects",
            "object_ids_from": {"dataset": "ads", "field": "creative.effective_object_story_id"},
            "fields": ["id", "permalink_url"],
        }, quick=False)
        self.assertEqual(x["object_ids_from"]["dataset"], "ads")

    def test_quick_read_paginated_collection(self):
        class Fake(MetaClient):
            def __init__(self):
                super().__init__({"a": "fake-long-test-token-example"})
            def discover(self):
                return {"accounts": [{"id": "100000", "token_alias": "a", "name": "A"}], "discovery_complete": True, "errors": []}
            def request(self, path, alias, params=None):
                if params.get("after") == "c2":
                    return {"data": [{"id": "2", "name": "B"}]}, {}
                return {"data": [{"id": "1", "name": "A"}], "paging": {"next": "x", "cursors": {"after": "c2"}}}, {}
        with patch.dict(os.environ, {"META_ACCOUNT_ALLOWLIST": "", "META_ACCOUNT_TOKEN_MAP_JSON": "{}"}):
            out = quick_read(Fake(), {"source": "campaigns", "fields": ["id", "name"], "row_limit": 10})
        self.assertEqual(out["row_count"], 2)
        self.assertEqual(out["rows"][1]["id"], "2")

    def test_generic_db_preserves_breakdown_rows(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite3"); os.close(fd)
        try:
            db = GenericDB(path)
            db.ensure_dataset({"name": "daily", "source": "insights"})
            rows = [
                {"ad_id": "1", "date_start": "2026-09-01", "gender": "male", "spend": "1"},
                {"ad_id": "1", "date_start": "2026-09-01", "gender": "female", "spend": "2"},
            ]
            db.insert_rows("daily", rows)
            n = db.db.execute("SELECT COUNT(*) FROM dataset_rows WHERE dataset='daily'").fetchone()[0]
            self.assertEqual(n, 2)
            db.close()
        finally:
            try: os.remove(path)
            except OSError: pass


if __name__ == "__main__":
    unittest.main()
