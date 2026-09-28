"""Resumable AI-directed generic Meta analysis jobs.

One ChatGPT tool call submits a declarative plan. The backend executes only the requested
read operations, persists raw datasets to private R2, and checkpoints under one logical job ID.
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
import urllib.request
from pathlib import Path
from typing import Any

from .generic_meta import ACCOUNT_COLLECTIONS, validate_plan, resolve_accounts, pages_for_account, object_rows, account_rows


def gateway_raw(path: str, method: str = "GET", data: bytes | None = None,
                mime: str = "application/octet-stream", timeout: int = 120) -> bytes:
    url = os.environ["GATEWAY_BASE_URL"].rstrip("/") + path
    headers = {"Authorization": "Bearer " + os.environ["JOB_SHARED_SECRET"], "User-Agent": "ElOkabyGenericJob/2.0"}
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
        try: minutes = int(os.getenv("HEAVY_RUN_BUDGET_MINUTES") or "300")
        except Exception: minutes = 300
        minutes = max(5, min(315, minutes))
        self.deadline = time.monotonic() + minutes * 60

    def low(self, reserve_seconds: int = 420) -> bool:
        return time.monotonic() >= self.deadline - reserve_seconds


class GenericDB:
    def __init__(self, path: str):
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
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
        """)
        self.db.commit()

    def set_meta(self, key: str, value: Any):
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, json.dumps(value, ensure_ascii=False, separators=(",",":"), default=str)))
        self.db.commit()

    def get_meta(self, key: str, default=None):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if not r: return default
        try: return json.loads(r[0])
        except Exception: return default

    def ensure_dataset(self, spec: dict):
        self.db.execute("INSERT OR IGNORE INTO datasets(name,source,spec_json,status,updated_at) VALUES(?,?,?,?,?)",
                        (spec["name"], spec["source"], json.dumps(spec, ensure_ascii=False, separators=(",",":")), "pending", _now()))
        self.db.commit()

    def progress(self, dataset: str, scope_id: str) -> tuple[str | None, str]:
        r = self.db.execute("SELECT cursor,status FROM progress WHERE dataset=? AND scope_id=?", (dataset, scope_id)).fetchone()
        if not r:
            self.db.execute("INSERT INTO progress(dataset,scope_id,status,updated_at) VALUES(?,?,?,?)", (dataset, scope_id, "pending", _now()))
            self.db.commit(); return None, "pending"
        return r["cursor"], r["status"]

    def set_progress(self, dataset: str, scope_id: str, cursor: str | None, status: str):
        self.db.execute("""INSERT INTO progress(dataset,scope_id,cursor,status,updated_at) VALUES(?,?,?,?,?)
          ON CONFLICT(dataset,scope_id) DO UPDATE SET cursor=excluded.cursor,status=excluded.status,updated_at=excluded.updated_at""",
          (dataset, scope_id, cursor, status, _now()))
        self.db.commit()

    def insert_rows(self, dataset: str, rows: list[dict]):
        idx = self.db.execute("SELECT COALESCE(MAX(row_index),0) FROM dataset_rows WHERE dataset=?", (dataset,)).fetchone()[0]
        inserted = 0
        for row in rows:
            raw = json.dumps(row, ensure_ascii=False, separators=(",",":"), default=str)
            key_material = "|".join(str(row.get(k) or "") for k in ("account_id","campaign_id","adset_id","ad_id","id","date_start","date_stop","__object_id"))
            if not key_material.strip("|"):
                key_material = raw
            # Include the full row so breakdown rows sharing the same entity/date do not collide.
            # Exact replay of the same page is still deduplicated after a checkpoint retry.
            row_key = hashlib.sha256((key_material + "|" + raw).encode("utf-8")).hexdigest()
            idx += 1
            cur = self.db.execute("INSERT OR IGNORE INTO dataset_rows(dataset,row_index,row_key,account_id,date_start,date_stop,data_json) VALUES(?,?,?,?,?,?,?)",
                (dataset, idx, row_key, str(row.get("account_id") or ""), str(row.get("date_start") or ""), str(row.get("date_stop") or ""), raw))
            inserted += int(cur.rowcount or 0)
        self.db.execute("UPDATE datasets SET row_count=(SELECT COUNT(*) FROM dataset_rows WHERE dataset=?),updated_at=? WHERE name=?", (dataset, _now(), dataset))
        self.db.commit()
        return inserted

    def error(self, dataset: str, scope_id: str, ex: Exception):
        msg = str(ex)[:1000]
        self.db.execute("INSERT INTO errors(dataset,scope_id,error,created_at) VALUES(?,?,?,?)", (dataset, scope_id, msg, _now()))
        self.db.commit()

    def close(self):
        self.db.commit(); self.db.close()


def _now(): return dt.datetime.now(dt.timezone.utc).isoformat()


def _load_checkpoint(job_id: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"elokaby_generic_{job_id}_", suffix=".sqlite3")
    os.close(fd)
    try:
        packed = gateway_raw(f"/internal/jobs/{job_id}/checkpoint", timeout=180)
        Path(path).write_bytes(gzip.decompress(packed))
    except RuntimeError as ex:
        if "HTTP 404" not in str(ex): raise
    return path


def _pack(path: str, level: int = 5) -> bytes:
    return gzip.compress(Path(path).read_bytes(), compresslevel=level)


def _checkpoint(job_id: str, db: GenericDB, reason: str, progress: dict):
    count = int(db.get_meta("continuation_count", 0)) + 1
    try: max_cont = int(os.getenv("HEAVY_MAX_CONTINUATIONS") or "30")
    except Exception: max_cont = 30
    if count > max_cont: raise RuntimeError("Generic job continuation safety limit reached")
    db.set_meta("continuation_count", count)
    db.set_meta("last_checkpoint_reason", reason)
    gateway_raw(f"/internal/jobs/{job_id}/checkpoint", "POST", _pack(db.path), "application/gzip", timeout=180)
    gateway_json(f"/internal/jobs/{job_id}/progress", "POST", {"progress": progress, "continuation_count": count})
    gateway_json(f"/internal/jobs/{job_id}/continue", "POST", {"reason": reason})
    raise ContinueRun()


def _dot(row: Any, path: str):
    cur=row
    for part in path.split("."):
        if isinstance(cur,dict):cur=cur.get(part)
        else:return None
    return cur


def _resolve_object_ids(db: GenericDB, spec: dict) -> list[str]:
    if spec.get("object_ids"): return list(dict.fromkeys(spec["object_ids"]))
    ref=spec.get("object_ids_from") or {}
    dataset,field=ref.get("dataset"),ref.get("field")
    if not dataset or not field: return []
    ids=[]
    for rec in db.db.execute("SELECT data_json FROM dataset_rows WHERE dataset=? ORDER BY row_index",(dataset,)):
        try:v=_dot(json.loads(rec[0]),field)
        except Exception:continue
        if isinstance(v,list): vals=v
        else: vals=[v]
        for x in vals:
            if x not in (None,""):ids.append(str(x))
    return list(dict.fromkeys(ids))


def _safe_checkpoint(job_id: str, db: GenericDB):
    gateway_raw(f"/internal/jobs/{job_id}/checkpoint", "POST", _pack(db.path, 4), "application/gzip", timeout=180)


def _run_account_dataset(job_id: str, db: GenericDB, budget: Budget, client, spec: dict, accounts: list[dict]):
    total_limit = spec["row_limit"]
    current_count = db.db.execute("SELECT COUNT(*) FROM dataset_rows WHERE dataset=?",(spec["name"],)).fetchone()[0]
    for account in accounts:
        aid=str(account["id"]); cursor,status=db.progress(spec["name"],aid)
        if status=="done": continue
        try:
            for page,nxt in pages_for_account(client,spec,account,after=cursor):
                remaining=total_limit-current_count
                if remaining<=0:
                    db.set_progress(spec["name"],aid,None,"done");break
                current_count += db.insert_rows(spec["name"],page[:remaining])
                if nxt:
                    db.set_progress(spec["name"],aid,nxt,"running")
                else:
                    db.set_progress(spec["name"],aid,None,"done")
                if budget.low():
                    _checkpoint(job_id,db,"time_budget",{"dataset":spec["name"],"account_id":aid,"rows":current_count})
                if not nxt or current_count>=total_limit: break
        except ContinueRun: raise
        except Exception as ex:
            db.error(spec["name"],aid,ex);db.set_progress(spec["name"],aid,None,"failed")
        if current_count>=total_limit: break
        _safe_checkpoint(job_id,db)


def _run_object_dataset(job_id: str, db: GenericDB, budget: Budget, client, spec: dict, account_hint: dict | None):
    ids=_resolve_object_ids(db,spec)
    if not ids: raise ValueError("No object IDs resolved for dataset")
    current=db.db.execute("SELECT COUNT(*) FROM dataset_rows WHERE dataset=?",(spec["name"],)).fetchone()[0]
    for oid in ids:
        _cursor,status=db.progress(spec["name"],oid)
        if status=="done":continue
        one=dict(spec);one["object_ids"]=[oid];one.pop("object_ids_from",None)
        try:
            rows=object_rows(client,one,account_hint)
            current += db.insert_rows(spec["name"],rows)
            db.set_progress(spec["name"],oid,None,"done")
        except Exception as ex:
            db.error(spec["name"],oid,ex);db.set_progress(spec["name"],oid,None,"failed")
        if budget.low(): _checkpoint(job_id,db,"time_budget",{"dataset":spec["name"],"object_id":oid,"rows":current})
        if current>=spec["row_limit"]: break
    _safe_checkpoint(job_id,db)


def _manifest(db: GenericDB) -> list[dict]:
    out=[]
    for r in db.db.execute("SELECT name,source,row_count,status,error FROM datasets ORDER BY rowid"):
        errors=db.db.execute("SELECT COUNT(*) FROM errors WHERE dataset=?",(r["name"],)).fetchone()[0]
        out.append({"name":r["name"],"source":r["source"],"row_count":r["row_count"],"status":r["status"],"error":r["error"],"error_count":errors})
    return out


def run_analysis_job(job: dict, client) -> tuple[dict | None, bytes | None, str | None, bool]:
    job_id=job["job_id"]; path=_load_checkpoint(job_id); db=GenericDB(path); budget=Budget()
    try:
        plan=db.get_meta("plan")
        if not plan:
            plan=validate_plan((job.get("input") or {}).get("params") or {})
            db.set_meta("plan",plan);db.set_meta("created_at",_now())
            for spec in plan["datasets"]:db.ensure_dataset(spec)
        # Discovery is performed once per logical job and persisted. Continuations reuse it.
        cached=db.get_meta("accounts")
        if cached is None:
            selected=plan["account_scope"].get("account_ids") or []
            accounts,discovery=resolve_accounts(client,selected)
            serial=[]
            for a in accounts:
                serial.append({k:v for k,v in a.items() if k not in ("token_aliases",)})
            db.set_meta("accounts",serial)
            db.set_meta("discovery",{k:v for k,v in discovery.items() if k not in ("accounts","usage_headers")})
            _safe_checkpoint(job_id,db)
        else:
            accounts=cached
        if not accounts: raise RuntimeError("No accessible Meta ad accounts discovered")
        account_hint=accounts[0] if accounts else None
        total=len(plan["datasets"])
        for idx,spec in enumerate(plan["datasets"],1):
            state=db.db.execute("SELECT status FROM datasets WHERE name=?",(spec["name"],)).fetchone()[0]
            if state=="done":continue
            gateway_json(f"/internal/jobs/{job_id}/progress","POST",{"progress":{"dataset":spec["name"],"dataset_index":idx,"total_datasets":total,"source":spec["source"]}})
            try:
                if spec["source"]=="accounts":
                    rows=account_rows(client,spec,accounts)
                    db.insert_rows(spec["name"],rows[:spec["row_limit"]])
                elif spec["source"] in ACCOUNT_COLLECTIONS or spec["source"]=="insights":
                    requested=spec.get("account_ids") or []
                    use=[a for a in accounts if not requested or str(a["id"]) in requested]
                    _run_account_dataset(job_id,db,budget,client,spec,use)
                else:
                    _run_object_dataset(job_id,db,budget,client,spec,account_hint)
                failures=db.db.execute("SELECT COUNT(*) FROM progress WHERE dataset=? AND status='failed'",(spec["name"],)).fetchone()[0]
                db.db.execute("UPDATE datasets SET status=?,updated_at=? WHERE name=?",("partial" if failures else "done",_now(),spec["name"]));db.db.commit()
            except ContinueRun: raise
            except Exception as ex:
                db.error(spec["name"],"dataset",ex)
                db.db.execute("UPDATE datasets SET status='failed',error=?,updated_at=? WHERE name=?",(str(ex)[:700],_now(),spec["name"]));db.db.commit()
            if budget.low(): _checkpoint(job_id,db,"time_budget_after_dataset",{"dataset":spec["name"],"dataset_index":idx,"total_datasets":total})
            _safe_checkpoint(job_id,db)
        manifest=_manifest(db)
        # Final durable dataset artifact; later query_job_data/aggregate_job_data read this without calling Meta.
        gateway_raw(f"/internal/jobs/{job_id}/dataset","POST",_pack(db.path,6),"application/gzip",timeout=240)
        previews={}
        n=plan["output"]["preview_rows_per_dataset"]
        if n:
            for m in manifest:
                vals=[]
                for rec in db.db.execute("SELECT data_json FROM dataset_rows WHERE dataset=? ORDER BY row_index LIMIT ?",(m["name"],n)):
                    try:vals.append(json.loads(rec[0]))
                    except Exception:pass
                previews[m["name"]]=vals
        status="COMPLETE" if all(x["status"]=="done" for x in manifest) else "PARTIAL"
        result={
          "analysis_job_status":status,
          "dataset_manifest":manifest,
          "account_count":len(accounts),
          "discovery":db.get_meta("discovery",{}),
          "previews":previews,
          "original_request":plan.get("original_request","")[:2000],
          "completed_at_utc":_now(),
          "important":"Raw persisted datasets are available through query_job_data / aggregate_job_data. Those tools read this stored job and do not re-query Meta."
        }
        return result,None,None,False
    except ContinueRun:
        return None,None,None,True
    finally:
        db.close()
        try:os.remove(path)
        except OSError:pass
