"""Read and aggregate persisted generic analysis-job datasets (v2.1 sharded storage).

These operations NEVER call Meta. They read the compact SQLite manifest plus any R2 gzip
JSONL shards produced by start_analysis_job. Query/aggregation is streaming; the full raw
dataset is never loaded into ChatGPT or memory at once.
"""
from __future__ import annotations

import gzip
import json
import os
import sqlite3
import tempfile
import urllib.parse
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from .generic_job import gateway_raw


def _load_parent(parent_job_id: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"elokaby_dataset_{parent_job_id}_", suffix=".sqlite3")
    os.close(fd)
    packed = gateway_raw(f"/internal/jobs/{parent_job_id}/dataset", timeout=180)
    Path(path).write_bytes(gzip.decompress(packed))
    return path


def _shard_url(parent_job_id: str, shard_key: str) -> str:
    return f"/internal/jobs/{parent_job_id}/shard?key={urllib.parse.quote(shard_key, safe='')}"


def _get_path(obj: Any, path: str):
    cur = obj
    for part in str(path).split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _cmp(value, op, target) -> bool:
    if op == "exists":
        return value is not None
    if op == "eq":
        return value == target
    if op == "ne":
        return value != target
    if op == "in":
        return value in (target if isinstance(target, list) else [target])
    if op == "contains":
        return str(target).lower() in str(value or "").lower()
    try:
        a, b = float(value), float(target)
        return {"gt": a > b, "gte": a >= b, "lt": a < b, "lte": a <= b}[op]
    except Exception:
        return False


def _matches(row: dict, filters: list[dict]) -> bool:
    for f in filters:
        if not _cmp(_get_path(row, f.get("field", "")), f.get("op", "eq"), f.get("value")):
            return False
    return True


def _action_value(row: dict, field: str, action_type: str) -> float:
    items = _get_path(row, field)
    if not isinstance(items, list):
        return 0.0
    total = 0.0
    for x in items:
        if isinstance(x, dict) and str(x.get("action_type")) == action_type:
            try:
                total += float(x.get("value") or 0)
            except Exception:
                pass
    return total


def _account_filter(filters: list[dict]) -> set[str] | None:
    for f in filters:
        if f.get("field") != "account_id":
            continue
        op, value = f.get("op"), f.get("value")
        if op == "eq":
            return {str(value)}
        if op == "in" and isinstance(value, list):
            return {str(x) for x in value}
    return None


def _iter_rows(parent_job_id: str, db: sqlite3.Connection, dataset: str, filters: list[dict]) -> Iterable[dict]:
    # Small/local rows.
    for rec in db.execute(
        "SELECT data_json FROM dataset_rows WHERE dataset=? ORDER BY row_index", (dataset,)
    ):
        try:
            yield json.loads(rec[0])
        except Exception:
            continue

    # Large sharded rows. Use shard metadata to skip unrelated accounts where possible.
    wanted_accounts = _account_filter(filters)
    sql = "SELECT shard_key,account_id FROM shards WHERE dataset=?"
    args: list[Any] = [dataset]
    if wanted_accounts:
        placeholders = ",".join("?" for _ in wanted_accounts)
        sql += f" AND (account_id IN ({placeholders}) OR account_id='_mixed')"
        args.extend(sorted(wanted_accounts))
    sql += " ORDER BY shard_key"

    for rec in db.execute(sql, args):
        packed = gateway_raw(_shard_url(parent_job_id, str(rec["shard_key"])), timeout=240)
        raw = gzip.decompress(packed)
        for line in raw.splitlines():
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _manifest(db: sqlite3.Connection) -> list[dict]:
    out = []
    for x in db.execute("SELECT name,source,row_count,status,error FROM datasets ORDER BY name"):
        shards = db.execute("SELECT COUNT(*) FROM shards WHERE dataset=?", (x["name"],)).fetchone()[0]
        out.append(
            {
                "name": x["name"],
                "source": x["source"],
                "row_count": x["row_count"],
                "status": x["status"],
                "error": x["error"],
                "storage": "r2_shards" if shards else "sqlite",
                "shard_count": int(shards or 0),
            }
        )
    return out


def query_dataset(parent_job_id: str, params: dict) -> dict:
    path = _load_parent(parent_job_id)
    db = None
    try:
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        dataset = str(params.get("dataset") or "")
        if not dataset:
            raise ValueError("dataset is required")
        fields = params.get("fields") or []
        filters = params.get("filters") or []
        if not isinstance(fields, list) or len(fields) > 100:
            raise ValueError("Invalid fields")
        if not isinstance(filters, list) or len(filters) > 20:
            raise ValueError("Invalid filters")
        limit = max(1, min(500, int(params.get("limit") or 100)))
        offset = max(0, int(params.get("offset") or 0))

        exists = db.execute("SELECT row_count FROM datasets WHERE name=?", (dataset,)).fetchone()
        if not exists:
            raise ValueError("Unknown dataset")
        dataset_row_count = int(exists[0] or 0)

        out, matched = [], 0
        for row in _iter_rows(parent_job_id, db, dataset, filters):
            if not _matches(row, filters):
                continue
            if matched < offset:
                matched += 1
                continue
            matched += 1
            if fields:
                row = {f: _get_path(row, f) for f in fields}
            out.append(row)
            if len(out) >= limit:
                break

        return {
            "parent_job_id": parent_job_id,
            "dataset": dataset,
            "rows": out,
            "returned": len(out),
            "offset": offset,
            "dataset_row_count": dataset_row_count,
            "manifest": _manifest(db),
            "storage": "streamed from persisted SQLite metadata + R2 shards; no Meta API call",
        }
    finally:
        try:
            if db is not None:
                db.close()
        except Exception:
            pass
        try:
            os.remove(path)
        except OSError:
            pass


def aggregate_dataset(parent_job_id: str, params: dict) -> dict:
    path = _load_parent(parent_job_id)
    db = None
    try:
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        dataset = str(params.get("dataset") or "")
        if not dataset:
            raise ValueError("dataset is required")
        group_by = params.get("group_by") or []
        metrics = params.get("metrics") or []
        filters = params.get("filters") or []
        if not isinstance(group_by, list) or len(group_by) > 8:
            raise ValueError("Invalid group_by")
        if not isinstance(metrics, list) or not metrics or len(metrics) > 30:
            raise ValueError("Invalid metrics")
        if not isinstance(filters, list) or len(filters) > 20:
            raise ValueError("Invalid filters")
        if not db.execute("SELECT 1 FROM datasets WHERE name=?", (dataset,)).fetchone():
            raise ValueError("Unknown dataset")

        groups = {}
        scanned = 0
        for row in _iter_rows(parent_job_id, db, dataset, filters):
            if not _matches(row, filters):
                continue
            scanned += 1
            key = tuple(_get_path(row, g) for g in group_by)
            g = groups.setdefault(
                key,
                {"__count": 0, "__values": defaultdict(list), "__sums": defaultdict(float), "__sets": defaultdict(set)},
            )
            g["__count"] += 1
            for m in metrics:
                name = str(m.get("name") or m.get("field") or m.get("op") or "metric")
                op = str(m.get("op") or "sum")
                if op in ("count", "ratio"):
                    continue
                if op == "action_sum":
                    val = _action_value(row, str(m.get("field") or "actions"), str(m.get("action_type") or ""))
                else:
                    val = _get_path(row, str(m.get("field") or ""))
                if op in ("sum", "avg", "min", "max", "action_sum"):
                    try:
                        fv = float(val or 0)
                    except Exception:
                        continue
                    g["__sums"][name] += fv
                    g["__values"][name].append(fv)
                elif op == "count_distinct":
                    if val is not None:
                        g["__sets"][name].add(str(val))

        rows = []
        for key, g in groups.items():
            out = {group_by[i]: key[i] for i in range(len(group_by))}
            for m in metrics:
                name = str(m.get("name") or m.get("field") or m.get("op") or "metric")
                op = str(m.get("op") or "sum")
                if op == "ratio":
                    continue
                if op == "count":
                    out[name] = g["__count"]
                elif op in ("sum", "action_sum"):
                    out[name] = g["__sums"].get(name, 0.0)
                elif op == "avg":
                    vals = g["__values"].get(name, [])
                    out[name] = sum(vals) / len(vals) if vals else None
                elif op == "min":
                    vals = g["__values"].get(name, [])
                    out[name] = min(vals) if vals else None
                elif op == "max":
                    vals = g["__values"].get(name, [])
                    out[name] = max(vals) if vals else None
                elif op == "count_distinct":
                    out[name] = len(g["__sets"].get(name, set()))
            for m in metrics:
                if str(m.get("op") or "sum") != "ratio":
                    continue
                name = str(m.get("name") or "ratio")
                num = str(m.get("numerator_metric") or "")
                den = str(m.get("denominator_metric") or "")
                out[name] = (out.get(num) / out.get(den)) if out.get(den) not in (None, 0) else None
            rows.append(out)

        sort_by = str(params.get("sort_by") or "")
        reverse = bool(params.get("descending", True))
        if sort_by:
            rows.sort(key=lambda x: (x.get(sort_by) is not None, x.get(sort_by) or 0), reverse=reverse)
        limit = max(1, min(500, int(params.get("limit") or 100)))
        return {
            "parent_job_id": parent_job_id,
            "dataset": dataset,
            "groups": rows[:limit],
            "group_count": len(rows),
            "returned": min(limit, len(rows)),
            "matched_rows_scanned": scanned,
            "storage": "streamed from persisted SQLite metadata + R2 shards; no Meta API call",
        }
    finally:
        try:
            if db is not None:
                db.close()
        except Exception:
            pass
        try:
            os.remove(path)
        except OSError:
            pass
