"""Resumable AI-directed generic Meta analysis jobs (v2.1 sharded storage).

One ChatGPT tool call submits a declarative plan. The backend executes only the requested
read operations. Large daily Insights datasets are persisted as many private R2 gzip shards,
while the SQLite checkpoint keeps only metadata/progress/small datasets. This prevents a
single checkpoint/final dataset file from growing with millions of daily rows.

No objective/Winner/agent business rules live here.
"""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import os
import sqlite3
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable

from .generic_meta import (
    ACCOUNT_COLLECTIONS,
    account_rows,
    object_rows,
    pages_for_account,
    resolve_accounts,
    validate_plan,
)

STORAGE_VERSION = 2
DEFAULT_SHARD_MAX_ROWS = 20_000
DEFAULT_SHARD_MAX_RAW_BYTES = 16 * 1024 * 1024


def gateway_raw(
    path: str,
    method: str = "GET",
    data: bytes | None = None,
    mime: str = "application/octet-stream",
    timeout: int = 120,
) -> bytes:
    url = os.environ["GATEWAY_BASE_URL"].rstrip("/") + path
    headers = {
        "Authorization": "Bearer " + os.environ["JOB_SHARED_SECRET"],
        "User-Agent": "ElOkabyGenericJob/2.1-sharded",
    }
    if data is not None:
        headers["Content-Type"] = mime
        headers["Content-Length"] = str(len(data))
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as ex:
        body = ex.read(1000).decode("utf-8", "replace")
        raise RuntimeError(f"Gateway HTTP {ex.code}: {body[:300]}") from None


def gateway_json(path: str, method: str = "GET", data: dict | None = None) -> dict:
    raw = None if data is None else json.dumps(data, ensure_ascii=False).encode("utf-8")
    b = gateway_raw(path, method, raw, "application/json")
    return json.loads(b) if b else {}


class ContinueRun(Exception):
    pass


class Budget:
    def __init__(self):
        try:
            minutes = int(os.getenv("HEAVY_RUN_BUDGET_MINUTES") or "300")
        except Exception:
            minutes = 300
        minutes = max(5, min(315, minutes))
        self.deadline = time.monotonic() + minutes * 60

    def low(self, reserve_seconds: int = 420) -> bool:
        return time.monotonic() >= self.deadline - reserve_seconds


class GenericDB:
    def __init__(self, path: str):
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(
            """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS datasets(
          name TEXT PRIMARY KEY, source TEXT NOT NULL, spec_json TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending', row_count INTEGER NOT NULL DEFAULT 0,
          error TEXT, updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS dataset_rows(
          dataset TEXT NOT NULL, row_index INTEGER NOT NULL, row_key TEXT NOT NULL,
          account_id TEXT, date_start TEXT, date_stop TEXT, data_json TEXT NOT NULL,
          PRIMARY KEY(dataset,row_key)
        );
        CREATE INDEX IF NOT EXISTS dataset_rows_order ON dataset_rows(dataset,row_index);
        CREATE INDEX IF NOT EXISTS dataset_rows_account ON dataset_rows(dataset,account_id);
        CREATE TABLE IF NOT EXISTS progress(
          dataset TEXT NOT NULL, scope_id TEXT NOT NULL, cursor TEXT,
          status TEXT NOT NULL DEFAULT 'pending', updated_at TEXT,
          PRIMARY KEY(dataset,scope_id)
        );
        CREATE TABLE IF NOT EXISTS errors(
          id INTEGER PRIMARY KEY AUTOINCREMENT,dataset TEXT,scope_id TEXT,error TEXT,created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS shards(
          dataset TEXT NOT NULL,
          shard_key TEXT PRIMARY KEY,
          scope_id TEXT,
          account_id TEXT,
          row_count INTEGER NOT NULL,
          date_start TEXT,
          date_stop TEXT,
          raw_bytes INTEGER NOT NULL DEFAULT 0,
          gzip_bytes INTEGER NOT NULL DEFAULT 0,
          created_at TEXT
        );
        CREATE INDEX IF NOT EXISTS shards_dataset ON shards(dataset,shard_key);
        CREATE INDEX IF NOT EXISTS shards_account ON shards(dataset,account_id);
        """
        )
        self.db.commit()

    def set_meta(self, key: str, value: Any):
        self.db.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)),
        )
        self.db.commit()

    def get_meta(self, key: str, default=None):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if not r:
            return default
        try:
            return json.loads(r[0])
        except Exception:
            return default

    def ensure_dataset(self, spec: dict):
        self.db.execute(
            "INSERT OR IGNORE INTO datasets(name,source,spec_json,status,updated_at) VALUES(?,?,?,?,?)",
            (
                spec["name"],
                spec["source"],
                json.dumps(spec, ensure_ascii=False, separators=(",", ":")),
                "pending",
                _now(),
            ),
        )
        self.db.commit()

    def dataset_count(self, dataset: str) -> int:
        r = self.db.execute("SELECT row_count FROM datasets WHERE name=?", (dataset,)).fetchone()
        return int(r[0] or 0) if r else 0

    def shard_count(self, dataset: str | None = None) -> int:
        if dataset:
            r = self.db.execute("SELECT COUNT(*) FROM shards WHERE dataset=?", (dataset,)).fetchone()
        else:
            r = self.db.execute("SELECT COUNT(*) FROM shards").fetchone()
        return int(r[0] or 0)

    def progress(self, dataset: str, scope_id: str) -> tuple[str | None, str]:
        r = self.db.execute(
            "SELECT cursor,status FROM progress WHERE dataset=? AND scope_id=?",
            (dataset, scope_id),
        ).fetchone()
        if not r:
            self.db.execute(
                "INSERT INTO progress(dataset,scope_id,status,updated_at) VALUES(?,?,?,?)",
                (dataset, scope_id, "pending", _now()),
            )
            self.db.commit()
            return None, "pending"
        return r["cursor"], r["status"]

    def set_progress(self, dataset: str, scope_id: str, cursor: str | None, status: str):
        self.db.execute(
            """INSERT INTO progress(dataset,scope_id,cursor,status,updated_at) VALUES(?,?,?,?,?)
          ON CONFLICT(dataset,scope_id) DO UPDATE SET cursor=excluded.cursor,status=excluded.status,updated_at=excluded.updated_at""",
            (dataset, scope_id, cursor, status, _now()),
        )
        self.db.commit()

    def insert_rows(self, dataset: str, rows: list[dict]):
        """Store small/non-sharded dataset rows inside the metadata SQLite file."""
        idx = self.db.execute(
            "SELECT COALESCE(MAX(row_index),0) FROM dataset_rows WHERE dataset=?",
            (dataset,),
        ).fetchone()[0]
        inserted = 0
        for row in rows:
            raw = json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str)
            key_material = "|".join(
                str(row.get(k) or "")
                for k in (
                    "account_id",
                    "campaign_id",
                    "adset_id",
                    "ad_id",
                    "id",
                    "date_start",
                    "date_stop",
                    "__object_id",
                )
            )
            if not key_material.strip("|"):
                key_material = raw
            row_key = hashlib.sha256((key_material + "|" + raw).encode("utf-8")).hexdigest()
            idx += 1
            cur = self.db.execute(
                "INSERT OR IGNORE INTO dataset_rows(dataset,row_index,row_key,account_id,date_start,date_stop,data_json) VALUES(?,?,?,?,?,?,?)",
                (
                    dataset,
                    idx,
                    row_key,
                    str(row.get("account_id") or ""),
                    str(row.get("date_start") or ""),
                    str(row.get("date_stop") or ""),
                    raw,
                ),
            )
            inserted += int(cur.rowcount or 0)

        self.db.execute(
            "UPDATE datasets SET row_count=(SELECT COUNT(*) FROM dataset_rows WHERE dataset=?),updated_at=? WHERE name=?",
            (dataset, _now(), dataset),
        )
        self.db.commit()
        return inserted

    def add_shard(
        self,
        *,
        dataset: str,
        shard_key: str,
        scope_id: str,
        account_id: str,
        row_count: int,
        date_start: str,
        date_stop: str,
        raw_bytes: int,
        gzip_bytes: int,
        increment_dataset: bool,
    ) -> bool:
        cur = self.db.execute(
            """INSERT OR IGNORE INTO shards(
                 dataset,shard_key,scope_id,account_id,row_count,date_start,date_stop,
                 raw_bytes,gzip_bytes,created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                dataset,
                shard_key,
                scope_id,
                account_id,
                int(row_count),
                date_start,
                date_stop,
                int(raw_bytes),
                int(gzip_bytes),
                _now(),
            ),
        )
        inserted = bool(cur.rowcount)
        if inserted and increment_dataset:
            self.db.execute(
                "UPDATE datasets SET row_count=row_count+?,updated_at=? WHERE name=?",
                (int(row_count), _now(), dataset),
            )
        self.db.commit()
        return inserted

    def error(self, dataset: str, scope_id: str, ex: Exception):
        self.db.execute(
            "INSERT INTO errors(dataset,scope_id,error,created_at) VALUES(?,?,?,?)",
            (dataset, scope_id, str(ex)[:1000], _now()),
        )
        self.db.commit()

    def close(self):
        self.db.commit()
        self.db.close()


def _now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        v = int(os.getenv(name) or default)
    except Exception:
        v = default
    return max(lo, min(hi, v))


def _load_checkpoint(job_id: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"elokaby_generic_{job_id}_", suffix=".sqlite3")
    os.close(fd)
    try:
        packed = gateway_raw(f"/internal/jobs/{job_id}/checkpoint", timeout=180)
        Path(path).write_bytes(gzip.decompress(packed))
    except RuntimeError as ex:
        if "HTTP 404" not in str(ex):
            raise
    return path


def _pack(path: str, level: int = 5) -> bytes:
    return gzip.compress(Path(path).read_bytes(), compresslevel=level)


def _shard_url(job_id: str, shard_key: str) -> str:
    return f"/internal/jobs/{job_id}/shard?key={urllib.parse.quote(shard_key, safe='')}"


def _should_shard(spec: dict) -> bool:
    # Daily/periodic Insights are the only datasets expected to grow into millions of rows.
    return spec.get("source") == "insights" and spec.get("time_increment") is not None


def _jsonl(rows: list[dict]) -> bytes:
    return ("\n".join(
        json.dumps(r, ensure_ascii=False, separators=(",", ":"), default=str) for r in rows
    ) + "\n").encode("utf-8")


def _raw_jsonl(raw_json_strings: list[str]) -> bytes:
    return ("\n".join(raw_json_strings) + "\n").encode("utf-8")


def _row_dates(rows: Iterable[dict]) -> tuple[str, str]:
    starts, stops = [], []
    for row in rows:
        if row.get("date_start"):
            starts.append(str(row["date_start"]))
        if row.get("date_stop"):
            stops.append(str(row["date_stop"]))
    return (min(starts) if starts else "", max(stops) if stops else (max(starts) if starts else ""))


def _upload_rows_shard(
    job_id: str,
    db: GenericDB,
    spec: dict,
    scope_id: str,
    account_id: str,
    rows: list[dict],
    *,
    increment_dataset: bool = True,
    key_prefix: str = "part",
) -> tuple[str | None, int]:
    if not rows:
        return None, 0
    raw = _jsonl(rows)
    packed = gzip.compress(raw, compresslevel=5)
    digest = hashlib.sha256(raw).hexdigest()[:24]
    scope_hash = hashlib.sha256(scope_id.encode("utf-8")).hexdigest()[:16]
    shard_key = (
        f"shards/{job_id}/{spec['name']}/{account_id or '_all'}/"
        f"{key_prefix}-{scope_hash}-{digest}.jsonl.gz"
    )
    gateway_raw(_shard_url(job_id, shard_key), "POST", packed, "application/gzip", timeout=240)
    ds, de = _row_dates(rows)
    db.add_shard(
        dataset=spec["name"],
        shard_key=shard_key,
        scope_id=scope_id,
        account_id=account_id,
        row_count=len(rows),
        date_start=ds,
        date_stop=de,
        raw_bytes=len(raw),
        gzip_bytes=len(packed),
        increment_dataset=increment_dataset,
    )
    return shard_key, len(rows)


def _upload_legacy_raw_shard(
    job_id: str,
    db: GenericDB,
    spec: dict,
    rows: list[sqlite3.Row],
) -> tuple[str, int]:
    raw_strings = [str(r["data_json"]) for r in rows]
    raw = _raw_jsonl(raw_strings)
    packed = gzip.compress(raw, compresslevel=5)
    digest = hashlib.sha256(raw).hexdigest()[:24]
    first_idx, last_idx = int(rows[0]["row_index"]), int(rows[-1]["row_index"])
    account_ids = {str(r["account_id"] or "") for r in rows}
    account_id = next(iter(account_ids)) if len(account_ids) == 1 else "_mixed"
    shard_key = (
        f"shards/{job_id}/{spec['name']}/{account_id or '_all'}/"
        f"legacy-{first_idx:012d}-{last_idx:012d}-{digest}.jsonl.gz"
    )
    gateway_raw(_shard_url(job_id, shard_key), "POST", packed, "application/gzip", timeout=240)
    ds = min((str(r["date_start"]) for r in rows if r["date_start"]), default="")
    de = max((str(r["date_stop"]) for r in rows if r["date_stop"]), default=ds)
    db.add_shard(
        dataset=spec["name"],
        shard_key=shard_key,
        scope_id="legacy_migration",
        account_id=account_id,
        row_count=len(rows),
        date_start=ds,
        date_stop=de,
        raw_bytes=len(raw),
        gzip_bytes=len(packed),
        increment_dataset=False,  # dataset.row_count already contains these legacy rows
    )
    return shard_key, len(rows)


def _report_progress(job_id: str, db: GenericDB, progress: dict):
    payload = dict(progress)
    payload.setdefault("continuation_count", int(db.get_meta("continuation_count", 0) or 0))
    gateway_json(
        f"/internal/jobs/{job_id}/progress",
        "POST",
        {"progress": payload, "continuation_count": payload["continuation_count"]},
    )


def _safe_checkpoint(job_id: str, db: GenericDB):
    packed = _pack(db.path, 4)
    # With sharded storage this should stay small. A guard gives a useful error if a future
    # dataset accidentally starts accumulating raw rows inside the checkpoint again.
    if len(packed) > 70_000_000:
        raise RuntimeError(
            "Metadata checkpoint unexpectedly exceeded 70 MB after sharding; inspect unsharded dataset_rows"
        )
    gateway_raw(
        f"/internal/jobs/{job_id}/checkpoint",
        "POST",
        packed,
        "application/gzip",
        timeout=180,
    )


def _checkpoint(job_id: str, db: GenericDB, reason: str, progress: dict):
    count = int(db.get_meta("continuation_count", 0)) + 1
    max_cont = _env_int("HEAVY_MAX_CONTINUATIONS", 30, 1, 200)
    if count > max_cont:
        raise RuntimeError("Generic job continuation safety limit reached")

    db.set_meta("continuation_count", count)
    db.set_meta("last_checkpoint_reason", reason)
    _safe_checkpoint(job_id, db)
    _report_progress(job_id, db, {**progress, "continuation_count": count})
    gateway_json(f"/internal/jobs/{job_id}/continue", "POST", {"reason": reason})
    raise ContinueRun()


def _legacy_migrate_to_shards(job_id: str, db: GenericDB, plan: dict):
    """Convert any v2.0 giant daily rows already inside a checkpoint into R2 shards.

    The original datasets.row_count values are preserved. Shard keys are deterministic, so if
    migration is retried from the same old checkpoint, existing R2 objects are safely overwritten
    rather than duplicated. Only after all large rows are externalized do we VACUUM and save a
    compact v2.1 checkpoint.
    """
    if int(db.get_meta("storage_version", 1) or 1) >= STORAGE_VERSION:
        return

    # Old v2.0 generic plans defaulted to 5M rows and the public schema capped at 10M.
    # For a clearly requested FULL HISTORICAL audit, promote only those legacy sharded limits
    # so recovery does not silently truncate at the old architecture ceiling. Explicit small
    # test/sample limits are left untouched.
    original_request = str(plan.get("original_request") or "").lower()
    if "full historical" in original_request:
        changed = False
        for sp in plan.get("datasets", []):
            if _should_shard(sp) and int(sp.get("row_limit") or 0) in (5_000_000, 10_000_000):
                sp["row_limit"] = 100_000_000
                db.db.execute(
                    "UPDATE datasets SET spec_json=?,updated_at=? WHERE name=?",
                    (json.dumps(sp, ensure_ascii=False, separators=(",", ":")), _now(), sp["name"]),
                )
                changed = True
        if changed:
            db.set_meta("plan", plan)
            db.db.commit()

    specs = {x["name"]: x for x in plan.get("datasets", [])}
    batch_rows = _env_int("LEGACY_MIGRATION_SHARD_ROWS", 12_000, 1_000, 50_000)

    for name, spec in specs.items():
        if not _should_shard(spec):
            continue
        total_local = db.db.execute(
            "SELECT COUNT(*) FROM dataset_rows WHERE dataset=?", (name,)
        ).fetchone()[0]
        if not total_local:
            continue

        migrated = 0
        while True:
            rows = db.db.execute(
                """SELECT row_index,account_id,date_start,date_stop,data_json
                   FROM dataset_rows WHERE dataset=? ORDER BY row_index LIMIT ?""",
                (name, batch_rows),
            ).fetchall()
            if not rows:
                break
            _upload_legacy_raw_shard(job_id, db, spec, rows)
            ids = [int(r["row_index"]) for r in rows]
            db.db.execute(
                "DELETE FROM dataset_rows WHERE dataset=? AND row_index>=? AND row_index<=?",
                (name, min(ids), max(ids)),
            )
            db.db.commit()
            migrated += len(rows)
            _report_progress(
                job_id,
                db,
                {
                    "phase": "migrating_legacy_checkpoint",
                    "dataset": name,
                    "migrated_rows": migrated,
                    "legacy_rows_total": int(total_local),
                    "shards": db.shard_count(name),
                },
            )

    db.set_meta("storage_version", STORAGE_VERSION)
    db.set_meta("storage_layout", "sqlite-metadata+r2-jsonl-gzip-shards")
    db.db.commit()
    # Deleted SQLite pages do not shrink the file until VACUUM. This is what turns the ~80 MB
    # compressed legacy checkpoint into a small metadata checkpoint.
    db.db.execute("VACUUM")
    db.db.commit()
    _safe_checkpoint(job_id, db)


def _dot(row: Any, path: str):
    cur = row
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _iter_shard_rows(job_id: str, db: GenericDB, dataset: str) -> Iterable[dict]:
    for rec in db.db.execute(
        "SELECT shard_key FROM shards WHERE dataset=? ORDER BY shard_key", (dataset,)
    ):
        packed = gateway_raw(_shard_url(job_id, str(rec[0])), timeout=240)
        raw = gzip.decompress(packed)
        for line in raw.splitlines():
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _iter_dataset_rows(job_id: str, db: GenericDB, dataset: str) -> Iterable[dict]:
    for rec in db.db.execute(
        "SELECT data_json FROM dataset_rows WHERE dataset=? ORDER BY row_index", (dataset,)
    ):
        try:
            yield json.loads(rec[0])
        except Exception:
            continue
    yield from _iter_shard_rows(job_id, db, dataset)


def _resolve_object_ids(job_id: str, db: GenericDB, spec: dict) -> list[str]:
    if spec.get("object_ids"):
        return list(dict.fromkeys(spec["object_ids"]))
    ref = spec.get("object_ids_from") or {}
    dataset, field = ref.get("dataset"), ref.get("field")
    if not dataset or not field:
        return []
    ids: list[str] = []
    for row in _iter_dataset_rows(job_id, db, dataset):
        v = _dot(row, field)
        vals = v if isinstance(v, list) else [v]
        for x in vals:
            if x not in (None, ""):
                ids.append(str(x))
    return list(dict.fromkeys(ids))


def _date_windows(since: str, until: str, days: int) -> list[tuple[str, str]]:
    start = dt.date.fromisoformat(since)
    end = dt.date.fromisoformat(until)
    if start > end:
        return []
    days = max(7, min(3660, int(days)))
    out = []
    current = start
    step = dt.timedelta(days=days - 1)
    one_day = dt.timedelta(days=1)
    while current <= end:
        stop = min(end, current + step)
        out.append((current.isoformat(), stop.isoformat()))
        current = stop + one_day
    return out


def _historical_window_days(spec: dict) -> int:
    ti = str(spec.get("time_increment") or "")
    env_name = "GENERIC_DAILY_WINDOW_DAYS" if ti == "1" else "GENERIC_INSIGHTS_WINDOW_DAYS"
    default = 90 if ti == "1" else 366
    return _env_int(env_name, default, 7, 3660)


def _account_insights_bounds(
    job_id: str,
    db: GenericDB,
    budget: Budget,
    client,
    spec: dict,
    account: dict,
) -> dict:
    aid = str(account["id"])
    key = f"insights_bounds::{spec['name']}::{aid}"
    cached = db.get_meta(key)
    if isinstance(cached, dict):
        return cached

    probe = dict(spec)
    probe.update(
        {
            "source": "insights",
            "level": "account",
            "fields": ["spend"],
            "breakdowns": [],
            "filtering": [],
            "time_increment": None,
            "time_range": {"mode": "maximum", "date_preset": "maximum"},
            "limit": 10,
            "page_limit": 2,
        }
    )
    earliest = None
    latest = None
    for page, _nxt in pages_for_account(client, probe, account):
        for row in page:
            ds, de = row.get("date_start"), row.get("date_stop")
            if ds and (earliest is None or ds < earliest):
                earliest = ds
            if de and (latest is None or de > latest):
                latest = de
        break

    bounds = {"since": earliest, "until": latest, "empty": not bool(earliest and latest)}
    db.set_meta(key, bounds)
    if budget.low():
        _checkpoint(
            job_id,
            db,
            "time_budget_after_bounds",
            {"dataset": spec["name"], "account_id": aid, "phase": "historical_bounds"},
        )
    return bounds


def _account_scopes(
    job_id: str,
    db: GenericDB,
    budget: Budget,
    client,
    spec: dict,
    account: dict,
) -> list[tuple[str, dict]]:
    aid = str(account["id"])
    if spec.get("source") != "insights" or not spec.get("time_increment"):
        return [(aid, spec)]

    tr = spec.get("time_range") or {}
    window_days = _historical_window_days(spec)
    if tr.get("mode") == "maximum":
        bounds = _account_insights_bounds(job_id, db, budget, client, spec, account)
        if bounds.get("empty"):
            db.set_progress(spec["name"], aid, None, "done")
            return []
        ranges = _date_windows(bounds["since"], bounds["until"], window_days)
    elif tr.get("mode") == "custom":
        ranges = _date_windows(tr["since"], tr["until"], window_days)
        if len(ranges) <= 1:
            return [(aid, spec)]
    else:
        return [(aid, spec)]

    scopes = []
    for since, until in ranges:
        scoped = dict(spec)
        scoped["time_range"] = {"mode": "custom", "since": since, "until": until}
        scopes.append((f"{aid}:{since}:{until}", scoped))
    return scopes


def _run_account_dataset(
    job_id: str,
    db: GenericDB,
    budget: Budget,
    client,
    spec: dict,
    accounts: list[dict],
    *,
    dataset_index: int,
    total_datasets: int,
):
    total_limit = spec["row_limit"]
    current_count = db.dataset_count(spec["name"])
    sharded = _should_shard(spec)
    max_rows = _env_int("GENERIC_SHARD_MAX_ROWS", DEFAULT_SHARD_MAX_ROWS, 1_000, 100_000)
    max_raw = _env_int(
        "GENERIC_SHARD_MAX_RAW_BYTES",
        DEFAULT_SHARD_MAX_RAW_BYTES,
        1_000_000,
        40_000_000,
    )
    windows_since_checkpoint = 0

    for account_index, account in enumerate(accounts, 1):
        aid = str(account["id"])
        scopes = _account_scopes(job_id, db, budget, client, spec, account)
        if not scopes:
            continue

        for scope_index, (scope_id, scoped_spec) in enumerate(scopes, 1):
            cursor, status = db.progress(spec["name"], scope_id)
            if status == "done":
                continue

            try:
                if sharded:
                    buffer: list[dict] = []
                    buffer_bytes = 0
                    committed_cursor = cursor
                    for page, nxt in pages_for_account(client, scoped_spec, account, after=cursor):
                        remaining = total_limit - current_count - len(buffer)
                        if remaining <= 0:
                            db.set_meta(f"row_limit_hit::{spec['name']}", True)
                            db.set_progress(spec["name"], scope_id, committed_cursor, "done")
                            break

                        clipped = page[:remaining]
                        buffer.extend(clipped)
                        buffer_bytes += sum(
                            len(json.dumps(r, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")) + 1
                            for r in clipped
                        )
                        hit_limit = current_count + len(buffer) >= total_limit
                        flush = (
                            len(buffer) >= max_rows
                            or buffer_bytes >= max_raw
                            or not nxt
                            or hit_limit
                            or budget.low()
                        )

                        if flush and buffer:
                            _, added = _upload_rows_shard(
                                job_id,
                                db,
                                spec,
                                scope_id,
                                aid,
                                buffer,
                            )
                            current_count += added
                            buffer = []
                            buffer_bytes = 0
                            committed_cursor = nxt
                            scope_status = "done" if (not nxt or hit_limit) else "running"
                            db.set_progress(spec["name"], scope_id, nxt if nxt else None, scope_status)
                            _report_progress(
                                job_id,
                                db,
                                {
                                    "dataset": spec["name"],
                                    "dataset_index": dataset_index,
                                    "total_datasets": total_datasets,
                                    "source": spec["source"],
                                    "account_id": aid,
                                    "account_index": account_index,
                                    "account_count": len(accounts),
                                    "scope": scope_id,
                                    "window_index": scope_index,
                                    "window_count": len(scopes),
                                    "rows": current_count,
                                    "shards": db.shard_count(spec["name"]),
                                    "date_since": (scoped_spec.get("time_range") or {}).get("since"),
                                    "date_until": (scoped_spec.get("time_range") or {}).get("until"),
                                },
                            )

                            if budget.low():
                                _checkpoint(
                                    job_id,
                                    db,
                                    "time_budget",
                                    {
                                        "dataset": spec["name"],
                                        "dataset_index": dataset_index,
                                        "total_datasets": total_datasets,
                                        "account_id": aid,
                                        "account_index": account_index,
                                        "account_count": len(accounts),
                                        "scope": scope_id,
                                        "window_index": scope_index,
                                        "window_count": len(scopes),
                                        "rows": current_count,
                                        "shards": db.shard_count(spec["name"]),
                                    },
                                )

                        if hit_limit:
                            db.set_meta(f"row_limit_hit::{spec['name']}", True)
                            break
                        if not nxt:
                            if not buffer:
                                db.set_progress(spec["name"], scope_id, None, "done")
                            break

                else:
                    for page, nxt in pages_for_account(client, scoped_spec, account, after=cursor):
                        remaining = total_limit - current_count
                        if remaining <= 0:
                            db.set_meta(f"row_limit_hit::{spec['name']}", True)
                            db.set_progress(spec["name"], scope_id, None, "done")
                            break
                        inserted = db.insert_rows(spec["name"], page[:remaining])
                        current_count += inserted
                        db.set_progress(
                            spec["name"], scope_id, nxt if nxt else None, "running" if nxt else "done"
                        )
                        if current_count >= total_limit:
                            db.set_meta(f"row_limit_hit::{spec['name']}", True)
                        if budget.low():
                            _checkpoint(
                                job_id,
                                db,
                                "time_budget",
                                {
                                    "dataset": spec["name"],
                                    "dataset_index": dataset_index,
                                    "total_datasets": total_datasets,
                                    "account_id": aid,
                                    "account_index": account_index,
                                    "account_count": len(accounts),
                                    "scope": scope_id,
                                    "window_index": scope_index,
                                    "window_count": len(scopes),
                                    "rows": current_count,
                                },
                            )
                        if not nxt or current_count >= total_limit:
                            break

            except ContinueRun:
                raise
            except Exception as ex:
                db.error(spec["name"], scope_id, ex)
                db.set_progress(spec["name"], scope_id, None, "failed")

            windows_since_checkpoint += 1
            if current_count >= total_limit:
                break
            if windows_since_checkpoint >= 5 or scope_index == len(scopes):
                _safe_checkpoint(job_id, db)
                windows_since_checkpoint = 0

        if current_count >= total_limit:
            break


def _run_object_dataset(
    job_id: str,
    db: GenericDB,
    budget: Budget,
    client,
    spec: dict,
    account_hint: dict | None,
):
    ids = _resolve_object_ids(job_id, db, spec)
    if not ids:
        raise ValueError("No object IDs resolved for dataset")
    current = db.dataset_count(spec["name"])
    for oid in ids:
        _cursor, status = db.progress(spec["name"], oid)
        if status == "done":
            continue
        one = dict(spec)
        one["object_ids"] = [oid]
        one.pop("object_ids_from", None)
        try:
            rows = object_rows(client, one, account_hint)
            current += db.insert_rows(spec["name"], rows)
            db.set_progress(spec["name"], oid, None, "done")
        except Exception as ex:
            db.error(spec["name"], oid, ex)
            db.set_progress(spec["name"], oid, None, "failed")
        if budget.low():
            _checkpoint(
                job_id,
                db,
                "time_budget",
                {"dataset": spec["name"], "object_id": oid, "rows": current},
            )
        if current >= spec["row_limit"]:
            db.set_meta(f"row_limit_hit::{spec['name']}", True)
            break
    _safe_checkpoint(job_id, db)


def _manifest(db: GenericDB) -> list[dict]:
    out = []
    for r in db.db.execute(
        "SELECT name,source,row_count,status,error,spec_json FROM datasets ORDER BY rowid"
    ):
        errors = db.db.execute(
            "SELECT COUNT(*) FROM errors WHERE dataset=?", (r["name"],)
        ).fetchone()[0]
        shards = db.shard_count(r["name"])
        try:
            spec = json.loads(r["spec_json"] or "{}")
        except Exception:
            spec = {}
        out.append(
            {
                "name": r["name"],
                "source": r["source"],
                "row_count": r["row_count"],
                "status": r["status"],
                "error": r["error"],
                "error_count": errors,
                "storage": "r2_shards" if shards else "sqlite",
                "shard_count": shards,
                "row_limit_hit": bool(db.get_meta(f"row_limit_hit::{r['name']}", False)),
                "time_increment": spec.get("time_increment"),
            }
        )
    return out


def _preview_dataset(job_id: str, db: GenericDB, dataset: str, n: int) -> list[dict]:
    vals = []
    for row in _iter_dataset_rows(job_id, db, dataset):
        vals.append(row)
        if len(vals) >= n:
            break
    return vals


def run_analysis_job(job: dict, client) -> tuple[dict | None, bytes | None, str | None, bool]:
    job_id = job["job_id"]
    path = _load_checkpoint(job_id)
    db = GenericDB(path)
    budget = Budget()

    try:
        plan = db.get_meta("plan")
        if not plan:
            plan = validate_plan((job.get("input") or {}).get("params") or {})
            db.set_meta("plan", plan)
            db.set_meta("created_at", _now())
            db.set_meta("storage_version", STORAGE_VERSION)
            db.set_meta("storage_layout", "sqlite-metadata+r2-jsonl-gzip-shards")
            for spec in plan["datasets"]:
                db.ensure_dataset(spec)
        else:
            # v2.0 checkpoints (including the failed ~80 MB checkpoint) are migrated once.
            _legacy_migrate_to_shards(job_id, db, plan)

        cached = db.get_meta("accounts")
        if cached is None:
            selected = plan["account_scope"].get("account_ids") or []
            accounts, discovery = resolve_accounts(client, selected)
            serial = [
                {k: v for k, v in a.items() if k not in ("token_aliases",)} for a in accounts
            ]
            db.set_meta("accounts", serial)
            db.set_meta(
                "discovery",
                {k: v for k, v in discovery.items() if k not in ("accounts", "usage_headers")},
            )
            _safe_checkpoint(job_id, db)
        else:
            accounts = cached

        if not accounts:
            raise RuntimeError("No accessible Meta ad accounts discovered")

        account_hint = accounts[0] if accounts else None
        total = len(plan["datasets"])

        for idx, spec in enumerate(plan["datasets"], 1):
            state = db.db.execute(
                "SELECT status FROM datasets WHERE name=?", (spec["name"],)
            ).fetchone()[0]
            if state == "done":
                continue

            _report_progress(
                job_id,
                db,
                {
                    "dataset": spec["name"],
                    "dataset_index": idx,
                    "total_datasets": total,
                    "source": spec["source"],
                    "rows": db.dataset_count(spec["name"]),
                    "shards": db.shard_count(spec["name"]),
                },
            )

            try:
                if spec["source"] == "accounts":
                    rows = account_rows(client, spec, accounts)
                    db.insert_rows(spec["name"], rows[: spec["row_limit"]])

                elif spec["source"] in ACCOUNT_COLLECTIONS or spec["source"] == "insights":
                    requested = spec.get("account_ids") or []
                    use = [
                        a for a in accounts if not requested or str(a["id"]) in requested
                    ]
                    _run_account_dataset(
                        job_id,
                        db,
                        budget,
                        client,
                        spec,
                        use,
                        dataset_index=idx,
                        total_datasets=total,
                    )

                else:
                    _run_object_dataset(job_id, db, budget, client, spec, account_hint)

                failures = db.db.execute(
                    "SELECT COUNT(*) FROM progress WHERE dataset=? AND status='failed'",
                    (spec["name"],),
                ).fetchone()[0]
                partial = bool(failures or db.get_meta(f"row_limit_hit::{spec['name']}", False))
                db.db.execute(
                    "UPDATE datasets SET status=?,updated_at=? WHERE name=?",
                    ("partial" if partial else "done", _now(), spec["name"]),
                )
                db.db.commit()

            except ContinueRun:
                raise
            except Exception as ex:
                db.error(spec["name"], "dataset", ex)
                db.db.execute(
                    "UPDATE datasets SET status='failed',error=?,updated_at=? WHERE name=?",
                    (str(ex)[:700], _now(), spec["name"]),
                )
                db.db.commit()

            if budget.low():
                _checkpoint(
                    job_id,
                    db,
                    "time_budget_after_dataset",
                    {
                        "dataset": spec["name"],
                        "dataset_index": idx,
                        "total_datasets": total,
                        "rows": db.dataset_count(spec["name"]),
                        "shards": db.shard_count(spec["name"]),
                    },
                )
            _safe_checkpoint(job_id, db)

        manifest = _manifest(db)
        # Final persisted dataset is now a compact metadata/index SQLite. Raw huge datasets remain
        # in R2 shards referenced by the shards table inside this file.
        gateway_raw(
            f"/internal/jobs/{job_id}/dataset",
            "POST",
            _pack(db.path, 6),
            "application/gzip",
            timeout=240,
        )

        previews = {}
        n = plan["output"]["preview_rows_per_dataset"]
        if n:
            for m in manifest:
                previews[m["name"]] = _preview_dataset(job_id, db, m["name"], n)

        status = "COMPLETE" if all(x["status"] == "done" for x in manifest) else "PARTIAL"
        result = {
            "analysis_job_status": status,
            "dataset_manifest": manifest,
            "account_count": len(accounts),
            "discovery": db.get_meta("discovery", {}),
            "storage": {
                "version": STORAGE_VERSION,
                "layout": "sqlite-metadata+r2-jsonl-gzip-shards",
                "total_shards": db.shard_count(),
                "note": "Large periodic Insights rows are stored in R2 shards; final SQLite stores metadata/indexes only.",
            },
            "previews": previews,
            "original_request": plan.get("original_request", "")[:2000],
            "completed_at_utc": _now(),
            "important": (
                "Raw persisted datasets are available through query_job_data / "
                "aggregate_job_data. Those tools stream stored R2 shards and do not re-query Meta."
            ),
        }
        return result, None, None, False

    except ContinueRun:
        return None, None, None, True
    finally:
        db.close()
        try:
            os.remove(path)
        except OSError:
            pass
