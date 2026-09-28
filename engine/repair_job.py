"""Targeted repair for a completed v2.1 start_analysis_job dataset.

This job DOES NOT restart the historical audit. It opens the already-persisted parent SQLite
manifest, retries only currently failed scopes, writes repaired rows back into the same parent
R2 dataset/shards, and refreshes the parent manifest. The repair itself is executed as a small
child orchestration job so the completed parent remains the canonical dataset ID.
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Iterable

from .generic_job import (
    GenericDB,
    _iter_dataset_rows,
    _manifest,
    _pack,
    _resolve_object_ids,
    _shard_url,
    _upload_rows_shard,
    gateway_json,
    gateway_raw,
)
from .generic_meta import pages_for_account


def _load_parent(parent_job_id: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"elokaby_repair_{parent_job_id}_", suffix=".sqlite3")
    os.close(fd)
    packed = gateway_raw(f"/internal/jobs/{parent_job_id}/dataset", timeout=180)
    Path(path).write_bytes(gzip.decompress(packed))
    return path


def _spec(db: GenericDB, dataset: str) -> dict:
    row = db.db.execute("SELECT spec_json FROM datasets WHERE name=?", (dataset,)).fetchone()
    if not row:
        raise ValueError(f"Unknown parent dataset: {dataset}")
    return json.loads(row[0] or "{}")


def _account_map(client) -> dict[str, dict]:
    return {str(x["id"]): x for x in client.discover().get("accounts", [])}


def _parse_scope(scope_id: str) -> tuple[str, str | None, str | None]:
    parts = str(scope_id or "").split(":")
    if len(parts) >= 3 and parts[0].isdigit():
        return parts[0], parts[1], parts[2]
    if parts and parts[0].isdigit():
        return parts[0], None, None
    return "", None, None


def _date_windows(since: str, until: str, days: int) -> list[tuple[str, str]]:
    start = dt.date.fromisoformat(since)
    end = dt.date.fromisoformat(until)
    out: list[tuple[str, str]] = []
    step = dt.timedelta(days=max(1, days) - 1)
    one = dt.timedelta(days=1)
    while start <= end:
        stop = min(end, start + step)
        out.append((start.isoformat(), stop.isoformat()))
        start = stop + one
    return out


def _progress(child_job_id: str, payload: dict):
    try:
        gateway_json(
            f"/internal/jobs/{child_job_id}/progress",
            "POST",
            {"progress": payload, "continuation_count": 0},
        )
    except Exception:
        pass


def _daily_key(row: dict) -> tuple[str, str, str, str, str, str]:
    return (
        str(row.get("account_id") or ""),
        str(row.get("campaign_id") or ""),
        str(row.get("adset_id") or ""),
        str(row.get("ad_id") or ""),
        str(row.get("date_start") or ""),
        str(row.get("date_stop") or ""),
    )


def _existing_daily_keys(parent_job_id: str, db: GenericDB) -> set[tuple[str, str, str, str, str, str]]:
    return {_daily_key(r) for r in _iter_dataset_rows(parent_job_id, db, "ad_daily")}


def _remove_scope_shards(parent_job_id: str, db: GenericDB, dataset: str, scope_id: str, daily_keys: set | None = None) -> int:
    rows = db.db.execute(
        "SELECT shard_key,row_count FROM shards WHERE dataset=? AND scope_id=?",
        (dataset, scope_id),
    ).fetchall()
    removed = sum(int(r["row_count"] or 0) for r in rows)
    for r in rows:
        shard_key = str(r["shard_key"])
        if daily_keys is not None and dataset == "ad_daily":
            try:
                packed = gateway_raw(_shard_url(parent_job_id, shard_key), timeout=120)
                for line in gzip.decompress(packed).splitlines():
                    if line:
                        daily_keys.discard(_daily_key(json.loads(line)))
            except Exception:
                pass
        try:
            gateway_raw(_shard_url(parent_job_id, shard_key), "DELETE", timeout=120)
        except Exception:
            # Orphaned old shard bytes are harmless if their manifest rows are removed.
            pass
    db.db.execute("DELETE FROM shards WHERE dataset=? AND scope_id=?", (dataset, scope_id))
    if removed:
        db.db.execute(
            "UPDATE datasets SET row_count=MAX(0,row_count-?),updated_at=? WHERE name=?",
            (removed, dt.datetime.now(dt.timezone.utc).isoformat(), dataset),
        )
    db.db.commit()
    return removed


def _remove_local_account_rows(db: GenericDB, dataset: str, account_id: str) -> int:
    before = db.db.execute(
        "SELECT COUNT(*) FROM dataset_rows WHERE dataset=? AND account_id=?",
        (dataset, account_id),
    ).fetchone()[0]
    db.db.execute(
        "DELETE FROM dataset_rows WHERE dataset=? AND account_id=?",
        (dataset, account_id),
    )
    db.db.execute(
        "UPDATE datasets SET row_count=(SELECT COUNT(*) FROM dataset_rows WHERE dataset=?),updated_at=? WHERE name=?",
        (dataset, dt.datetime.now(dt.timezone.utc).isoformat(), dataset),
    )
    db.db.commit()
    return int(before or 0)


def _clear_scope_errors(db: GenericDB, dataset: str, scope_id: str):
    db.db.execute("DELETE FROM errors WHERE dataset=? AND scope_id=?", (dataset, scope_id))
    db.db.commit()


def _clear_stale_done_errors(db: GenericDB):
    # Errors from an earlier failed attempt should not keep counting after that exact scope later
    # completed successfully.
    db.db.execute(
        """DELETE FROM errors
           WHERE EXISTS (
             SELECT 1 FROM progress p
             WHERE p.dataset=errors.dataset AND p.scope_id=errors.scope_id AND p.status='done'
           )"""
    )
    db.db.commit()


def _collect_pages(client, spec: dict, account: dict) -> list[dict]:
    rows: list[dict] = []
    for page, nxt in pages_for_account(client, spec, account):
        rows.extend(page)
        if not nxt:
            break
    return rows


def _fetch_daily_segment(client, base_spec: dict, account: dict, since: str, until: str) -> list[dict]:
    sp = dict(base_spec)
    sp["time_range"] = {"mode": "custom", "since": since, "until": until}
    sp["limit"] = min(200, int(sp.get("limit") or 200))
    return _collect_pages(client, sp, account)


def _repair_daily_scope(
    child_job_id: str,
    parent_job_id: str,
    db: GenericDB,
    client,
    spec: dict,
    account: dict,
    scope_id: str,
    since: str,
    until: str,
    daily_keys: set[tuple[str, str, str, str, str, str]],
) -> tuple[bool, int, list[dict]]:
    """Retry a failed 90d scope as 30d -> 7d -> 1d only where needed."""
    _remove_scope_shards(parent_job_id, db, spec["name"], scope_id, daily_keys)
    _clear_scope_errors(db, spec["name"], scope_id)
    repaired_rows = 0
    failures: list[dict] = []

    def only_new(rows: list[dict]) -> tuple[list[dict], list[tuple[str, str, str, str, str, str]]]:
        fresh = []
        keys = []
        pending = set()
        for row in rows:
            key = _daily_key(row)
            if key in daily_keys or key in pending:
                continue
            pending.add(key)
            keys.append(key)
            fresh.append(row)
        return fresh, keys

    def run_segment(a: str, b: str, fallback_days: int | None) -> bool:
        nonlocal repaired_rows
        try:
            rows, new_keys = only_new(_fetch_daily_segment(client, spec, account, a, b))
            if rows:
                _, n = _upload_rows_shard(
                    parent_job_id,
                    db,
                    spec,
                    scope_id,
                    str(account["id"]),
                    rows,
                    key_prefix=f"repair-{a}-{b}",
                )
                repaired_rows += n
                daily_keys.update(new_keys)
            return True
        except Exception as ex:
            if fallback_days is None:
                failures.append({"since": a, "until": b, "error": str(ex)[:300]})
                return False
            child_windows = _date_windows(a, b, fallback_days)
            next_fallback = 1 if fallback_days == 7 else None
            ok = True
            for sa, sb in child_windows:
                # Two tries at the smallest level handles transient 400/500 responses without
                # multiplying a full historical extraction.
                if next_fallback is None:
                    last = None
                    for attempt in range(2):
                        try:
                            rows, new_keys = only_new(_fetch_daily_segment(client, spec, account, sa, sb))
                            if rows:
                                _, n = _upload_rows_shard(
                                    parent_job_id,
                                    db,
                                    spec,
                                    scope_id,
                                    str(account["id"]),
                                    rows,
                                    key_prefix=f"repair-{sa}-{sb}",
                                )
                                repaired_rows += n
                                daily_keys.update(new_keys)
                            last = None
                            break
                        except Exception as iex:
                            last = iex
                            if attempt == 0:
                                time.sleep(2)
                    if last is not None:
                        failures.append({"since": sa, "until": sb, "error": str(last)[:300]})
                        ok = False
                else:
                    if not run_segment(sa, sb, next_fallback):
                        ok = False
            return ok

    all_ok = True
    for a, b in _date_windows(since, until, 30):
        if not run_segment(a, b, 7):
            all_ok = False
        _progress(
            child_job_id,
            {
                "phase": "repair_daily",
                "dataset": spec["name"],
                "account_id": str(account["id"]),
                "scope": scope_id,
                "segment_since": a,
                "segment_until": b,
                "rows_repaired": repaired_rows,
                "remaining_failures": len(failures),
            },
        )

    if all_ok and not failures:
        db.set_progress(spec["name"], scope_id, None, "done")
    else:
        db.set_progress(spec["name"], scope_id, None, "failed")
        for f in failures:
            db.error(
                spec["name"],
                scope_id,
                RuntimeError(f"Repair subwindow {f['since']}..{f['until']}: {f['error']}"),
            )
    return all_ok and not failures, repaired_rows, failures


def _repair_creative_account(
    child_job_id: str,
    db: GenericDB,
    client,
    spec: dict,
    account: dict,
    scope_id: str,
) -> tuple[bool, int, str | None]:
    aid = str(account["id"])
    _remove_local_account_rows(db, spec["name"], aid)
    _clear_scope_errors(db, spec["name"], scope_id)

    last_error: Exception | None = None
    for page_size in (25, 10, 5, 1):
        trial = dict(spec)
        trial["limit"] = page_size
        try:
            rows = _collect_pages(client, trial, account)
            inserted = db.insert_rows(spec["name"], rows)
            db.set_progress(spec["name"], scope_id, None, "done")
            _progress(
                child_job_id,
                {
                    "phase": "repair_creatives",
                    "dataset": spec["name"],
                    "account_id": aid,
                    "page_size": page_size,
                    "rows_repaired": inserted,
                },
            )
            return True, inserted, None
        except Exception as ex:
            last_error = ex
            time.sleep(1)

    db.set_progress(spec["name"], scope_id, None, "failed")
    db.error(spec["name"], scope_id, last_error or RuntimeError("Creative repair failed"))
    return False, 0, str(last_error)[:300] if last_error else "Creative repair failed"


def _fallback_story_ids(parent_job_id: str, db: GenericDB) -> list[str]:
    vals: list[str] = []
    for row in _iter_dataset_rows(parent_job_id, db, "adcreatives"):
        for key in ("effective_object_story_id", "object_story_id"):
            v = row.get(key)
            if v:
                vals.append(str(v))
    return list(dict.fromkeys(vals))


def _repair_post_objects(
    child_job_id: str,
    parent_job_id: str,
    db: GenericDB,
    client,
) -> dict:
    row = db.db.execute("SELECT 1 FROM datasets WHERE name='post_objects'").fetchone()
    if not row:
        return {"attempted": False, "reason": "dataset_not_present"}

    spec = _spec(db, "post_objects")
    db.db.execute("DELETE FROM errors WHERE dataset='post_objects'")
    db.db.execute("DELETE FROM dataset_rows WHERE dataset='post_objects'")
    db.db.execute("DELETE FROM progress WHERE dataset='post_objects'")
    db.db.execute(
        "UPDATE datasets SET row_count=0,status='pending',error=NULL,updated_at=? WHERE name='post_objects'",
        (dt.datetime.now(dt.timezone.utc).isoformat(),),
    )
    db.db.commit()

    ids = _resolve_object_ids(parent_job_id, db, spec)
    if not ids:
        ids = _fallback_story_ids(parent_job_id, db)
    if not ids:
        db.db.execute(
            "UPDATE datasets SET status='failed',error='No object IDs resolved after creative repair',updated_at=? WHERE name='post_objects'",
            (dt.datetime.now(dt.timezone.utc).isoformat(),),
        )
        db.error("post_objects", "dataset", RuntimeError("No object IDs resolved after creative repair"))
        return {"attempted": True, "resolved_ids": 0, "success": 0, "failed": 0}

    fields = list(spec.get("fields") or ["id", "permalink_url"])
    aliases = list(client.tokens)
    success = failed = 0
    max_objects = min(len(ids), int(os.getenv("REPAIR_POST_OBJECT_LIMIT") or "5000"))
    selected = ids[:max_objects]
    resolved: dict[str, dict] = {}

    # Graph multi-ID reads keep this bounded: ~25 objects per API call rather than one request
    # per post. We try each configured token alias and merge what each token can see.
    batch_size = 25
    for offset in range(0, len(selected), batch_size):
        chunk = selected[offset: offset + batch_size]
        chunk_map: dict[str, dict] = {}
        for alias in aliases:
            try:
                payload, _ = client.request(
                    "/",
                    alias,
                    {"ids": ",".join(chunk), "fields": ",".join(fields)},
                )
                if isinstance(payload, dict):
                    for oid, item in payload.items():
                        if isinstance(item, dict) and not item.get("error"):
                            chunk_map[str(oid)] = item
            except Exception:
                continue
        resolved.update(chunk_map)
        idx = min(offset + batch_size, len(selected))
        _progress(
            child_job_id,
            {
                "phase": "repair_post_objects",
                "dataset": "post_objects",
                "object_index": idx,
                "object_count": len(selected),
                "batch_resolved": len(chunk_map),
                "resolved_total": len(resolved),
            },
        )

    # Retry only the unresolved minority individually so one inaccessible/deleted object does not
    # invalidate an otherwise successful batch.
    for oid in selected:
        if oid in resolved:
            continue
        last_error = None
        for alias in aliases:
            try:
                payload, _ = client.request(f"/{oid}", alias, {"fields": ",".join(fields)})
                if isinstance(payload, dict) and not payload.get("error"):
                    resolved[oid] = payload
                    last_error = None
                    break
            except Exception as ex:
                last_error = ex
        if oid not in resolved:
            db.set_progress("post_objects", oid, None, "failed")
            db.error("post_objects", oid, last_error or RuntimeError("Post object unavailable"))
            failed += 1

    for oid, payload in resolved.items():
        x = dict(payload)
        x.setdefault("__object_id", oid)
        db.insert_rows("post_objects", [x])
        db.set_progress("post_objects", oid, None, "done")
        success += 1

    # IDs beyond the configured safety limit stay explicitly partial rather than being silently
    # treated as covered.
    omitted = max(0, len(ids) - max_objects)
    status = "done" if failed == 0 and omitted == 0 else "partial"
    db.db.execute(
        "UPDATE datasets SET status=?,error=NULL,updated_at=? WHERE name='post_objects'",
        (status, dt.datetime.now(dt.timezone.utc).isoformat()),
    )
    db.db.commit()
    return {
        "attempted": True,
        "resolved_ids": len(ids),
        "attempted_ids": max_objects,
        "success": success,
        "failed": failed,
        "omitted_by_safety_limit": omitted,
        "status": status,
    }

def _update_dataset_status(db: GenericDB, dataset: str):
    failed = db.db.execute(
        "SELECT COUNT(*) FROM progress WHERE dataset=? AND status='failed'", (dataset,)
    ).fetchone()[0]
    status = "partial" if failed else "done"
    db.db.execute(
        "UPDATE datasets SET status=?,error=NULL,updated_at=? WHERE name=?",
        (status, dt.datetime.now(dt.timezone.utc).isoformat(), dataset),
    )
    db.db.commit()


def run_repair_job(job: dict, client) -> tuple[dict, None, None, bool]:
    child_job_id = job["job_id"]
    params = (job.get("input") or {}).get("params") or {}
    parent_job_id = str(params.get("parent_job_id") or "")
    if not parent_job_id:
        raise ValueError("parent_job_id is required")

    repair_daily = bool(params.get("repair_daily", True))
    repair_creatives = bool(params.get("repair_creatives", True))
    repair_posts = bool(params.get("repair_posts", True))
    path = _load_parent(parent_job_id)
    db = GenericDB(path)
    try:
        accounts = _account_map(client)
        summary = {
            "parent_job_id": parent_job_id,
            "daily": {"attempted_scopes": 0, "fixed_scopes": 0, "failed_scopes": 0, "rows_repaired": 0},
            "creatives": {"attempted_accounts": 0, "fixed_accounts": 0, "failed_accounts": 0, "rows_repaired": 0},
            "post_objects": {"attempted": False},
        }

        if repair_daily and db.db.execute("SELECT 1 FROM datasets WHERE name='ad_daily'").fetchone():
            spec = _spec(db, "ad_daily")
            daily_keys = _existing_daily_keys(parent_job_id, db)
            scopes = db.db.execute(
                "SELECT scope_id FROM progress WHERE dataset='ad_daily' AND status='failed' ORDER BY scope_id"
            ).fetchall()
            total = len(scopes)
            for idx, rec in enumerate(scopes, 1):
                scope_id = str(rec[0])
                aid, since, until = _parse_scope(scope_id)
                summary["daily"]["attempted_scopes"] += 1
                if not aid or not since or not until or aid not in accounts:
                    summary["daily"]["failed_scopes"] += 1
                    continue
                ok, n, failures = _repair_daily_scope(
                    child_job_id, parent_job_id, db, client, spec, accounts[aid], scope_id, since, until, daily_keys
                )
                summary["daily"]["rows_repaired"] += n
                if ok:
                    summary["daily"]["fixed_scopes"] += 1
                else:
                    summary["daily"]["failed_scopes"] += 1
                _progress(
                    child_job_id,
                    {
                        "phase": "repair_daily",
                        "scope_index": idx,
                        "scope_count": total,
                        "scope": scope_id,
                        "fixed": summary["daily"]["fixed_scopes"],
                        "failed": summary["daily"]["failed_scopes"],
                        "rows_repaired": summary["daily"]["rows_repaired"],
                    },
                )
            _update_dataset_status(db, "ad_daily")

        if repair_creatives and db.db.execute("SELECT 1 FROM datasets WHERE name='adcreatives'").fetchone():
            spec = _spec(db, "adcreatives")
            scopes = db.db.execute(
                "SELECT scope_id FROM progress WHERE dataset='adcreatives' AND status='failed' ORDER BY scope_id"
            ).fetchall()
            total = len(scopes)
            for idx, rec in enumerate(scopes, 1):
                scope_id = str(rec[0])
                aid, _s, _u = _parse_scope(scope_id)
                summary["creatives"]["attempted_accounts"] += 1
                if not aid or aid not in accounts:
                    summary["creatives"]["failed_accounts"] += 1
                    continue
                ok, n, err = _repair_creative_account(
                    child_job_id, db, client, spec, accounts[aid], scope_id
                )
                summary["creatives"]["rows_repaired"] += n
                if ok:
                    summary["creatives"]["fixed_accounts"] += 1
                else:
                    summary["creatives"]["failed_accounts"] += 1
                _progress(
                    child_job_id,
                    {
                        "phase": "repair_creatives",
                        "account_index": idx,
                        "account_count": total,
                        "account_id": aid,
                        "fixed": summary["creatives"]["fixed_accounts"],
                        "failed": summary["creatives"]["failed_accounts"],
                        "rows_repaired": summary["creatives"]["rows_repaired"],
                        **({"last_error": err} if err else {}),
                    },
                )
            _update_dataset_status(db, "adcreatives")

        if repair_posts:
            summary["post_objects"] = _repair_post_objects(
                child_job_id, parent_job_id, db, client
            )

        _clear_stale_done_errors(db)
        manifest = _manifest(db)
        current_failed = db.db.execute("SELECT COUNT(*) FROM progress WHERE status='failed'").fetchone()[0]
        current_errors = db.db.execute("SELECT COUNT(*) FROM errors").fetchone()[0]
        status = "COMPLETE" if all(x["status"] == "done" for x in manifest) else "PARTIAL"

        # Overwrite the SAME parent persisted dataset/index. This is the only merge step; callers
        # continue using the original parent_job_id for all later analysis.
        gateway_raw(
            f"/internal/jobs/{parent_job_id}/dataset",
            "POST",
            _pack(db.path, 6),
            "application/gzip",
            timeout=240,
        )

        parent_refresh = {
            "analysis_job_status": status,
            "dataset_manifest": manifest,
            "repair_summary": summary,
            "repair_completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "current_failed_scopes": int(current_failed or 0),
            "current_error_records": int(current_errors or 0),
        }
        gateway_json(
            f"/internal/jobs/{parent_job_id}/sync-analysis-result",
            "POST",
            parent_refresh,
        )

        return {
            **parent_refresh,
            "parent_job_id": parent_job_id,
            "storage_note": "Repaired rows were merged directly into the existing parent R2 dataset; no full re-extraction was run.",
        }, None, None, False
    finally:
        db.close()
        try:
            os.remove(path)
        except OSError:
            pass
