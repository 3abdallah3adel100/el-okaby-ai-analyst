"""Resumable full-history Lead Generation audit for El Okaby AI Analyst.

This module is deliberately self-contained and uses only the Python stdlib + XlsxWriter.
A single MCP call starts one logical audit job. Long scans checkpoint to private R2 via the
Cloudflare gateway and re-dispatch the same job ID before the GitHub runner timeout.
"""
from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import math
import os
import sqlite3
import statistics
import tempfile
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

from .meta import MetaClient, MetaError, clean_account

MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
PHASES = ["campaigns", "adsets", "ads", "lifetime", "daily", "finalize", "done"]
LEAD_OBJECTIVES = {"LEAD_GENERATION", "OUTCOME_LEADS"}
MESSAGE_MARKERS = ("messaging", "message", "conversation", "whatsapp", "messenger")


class ContinueRun(Exception):
    """Signal that a safe checkpoint was created and the same logical job must continue."""


def _gateway_raw(path: str, method: str = "GET", data: bytes | None = None,
                 mime: str = "application/octet-stream", timeout: int = 120) -> bytes:
    url = os.environ["GATEWAY_BASE_URL"].rstrip("/") + path
    headers = {
        "Authorization": "Bearer " + os.environ["JOB_SHARED_SECRET"],
        "User-Agent": "ElOkabyHeavyAudit/1.0",
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


def _gateway_json(path: str, method: str = "GET", data: dict | None = None) -> dict:
    raw = None if data is None else json.dumps(data, ensure_ascii=False).encode("utf-8")
    b = _gateway_raw(path, method, raw, "application/json")
    return json.loads(b) if b else {}


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(float(v or 0))
    except (TypeError, ValueError):
        return 0


def _json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"), default=str)


def _loads(v: str | None, default: Any):
    try:
        return json.loads(v) if v else default
    except Exception:
        return default


def extract_leads(actions: Any) -> tuple[float, str | None, list[dict]]:
    """Select one lead event without summing overlapping lead action families.

    Meta often reports multiple representations of the same conversion. We keep raw actions,
    explicitly exclude messaging/conversation events, prefer the canonical `lead` event, then
    choose the highest-valued lead-like event rather than double counting several aliases.
    """
    if not isinstance(actions, list):
        return 0.0, None, []
    candidates = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        typ = str(a.get("action_type") or "")
        low = typ.lower()
        if "lead" not in low or any(m in low for m in MESSAGE_MARKERS):
            continue
        val = _safe_float(a.get("value"))
        candidates.append({"action_type": typ, "value": val})
    if not candidates:
        return 0.0, None, []
    by_type = {x["action_type"]: x["value"] for x in candidates}
    for preferred in ("lead", "onsite_conversion.lead_grouped", "leadgen_grouped"):
        if preferred in by_type:
            return by_type[preferred], preferred, candidates
    best = max(candidates, key=lambda x: x["value"])
    return best["value"], best["action_type"], candidates


def _creative_fields(spec: dict, asset_feed: dict) -> dict:
    spec = spec if isinstance(spec, dict) else {}
    asset_feed = asset_feed if isinstance(asset_feed, dict) else {}
    page_id = str(spec.get("page_id") or "")
    block = spec.get("video_data") or spec.get("link_data") or spec.get("photo_data") or spec.get("template_data") or {}
    if not isinstance(block, dict):
        block = {}
    primary = block.get("message") or block.get("link_description") or ""
    headline = block.get("name") or block.get("title") or ""
    description = block.get("description") or ""
    cta = block.get("call_to_action") if isinstance(block.get("call_to_action"), dict) else {}
    cta_type = cta.get("type") or ""
    cta_value = cta.get("value") if isinstance(cta.get("value"), dict) else {}
    form_id = str(cta_value.get("lead_gen_form_id") or block.get("lead_gen_form_id") or "")
    children = block.get("child_attachments") if isinstance(block.get("child_attachments"), list) else []
    if asset_feed:
        ctype = "Dynamic Creative"
    elif spec.get("video_data"):
        ctype = "Video"
    elif len(children) > 1:
        ctype = "Carousel"
    elif spec.get("photo_data") or spec.get("link_data"):
        ctype = "Image/Link"
    else:
        ctype = "Other"
    return {
        "creative_type": ctype,
        "primary_text": str(primary or ""),
        "headline": str(headline or ""),
        "description": str(description or ""),
        "cta": str(cta_type or ""),
        "page_id": page_id,
        "form_id": form_id,
    }


def _post_link(story_id: str | None) -> str:
    story = str(story_id or "")
    if "_" not in story:
        return "Unavailable"
    _, post_id = story.split("_", 1)
    return f"https://www.facebook.com/{post_id}" if post_id.isdigit() else "Unavailable"


def _ad_link(account_id: str, ad_id: str) -> str:
    return f"https://www.facebook.com/adsmanager/manage/ads?act={account_id}&selected_ad_ids={ad_id}"


def _conversion_type(objective: str, optimization_goal: str, destination_type: str, action_type: str | None) -> str:
    joined = " ".join([objective or "", optimization_goal or "", destination_type or "", action_type or ""]).lower()
    if any(m in joined for m in MESSAGE_MARKERS):
        return "Messaging / excluded"
    if action_type and (action_type.startswith("offsite_conversion") or "pixel" in action_type.lower()):
        return "Website Lead"
    if "website" in (destination_type or "").lower():
        return "Website Lead"
    if objective in LEAD_OBJECTIVES or "lead" in (optimization_goal or "").lower() or (action_type and "lead" in action_type.lower()):
        return "Meta / Instant Form Lead"
    return "Other Lead Generation"


def _is_lead_candidate(objective: str, optimization_goal: str, destination_type: str, actions: Any) -> bool:
    joined = " ".join([objective or "", optimization_goal or "", destination_type or ""]).lower()
    leads, _, _ = extract_leads(actions)
    if leads > 0:
        return True
    if objective in LEAD_OBJECTIVES:
        return True
    if "lead" in joined and not any(m in joined for m in MESSAGE_MARKERS):
        return True
    return False


class AuditDB:
    def __init__(self, path: str):
        self.path = path
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self._schema()

    def _schema(self):
        self.db.executescript("""
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS accounts(
          account_id TEXT PRIMARY KEY, account_name TEXT, business_id TEXT, business_name TEXT,
          account_status TEXT, currency TEXT, timezone TEXT, token_alias TEXT,
          scan_status TEXT DEFAULT 'pending', earliest_date TEXT, latest_date TEXT,
          total_ads INTEGER DEFAULT 0, lead_ads INTEGER DEFAULT 0, error TEXT
        );
        CREATE TABLE IF NOT EXISTS campaigns(
          account_id TEXT, campaign_id TEXT, name TEXT, status TEXT, effective_status TEXT, objective TEXT,
          PRIMARY KEY(account_id,campaign_id)
        );
        CREATE TABLE IF NOT EXISTS adsets(
          account_id TEXT, adset_id TEXT, campaign_id TEXT, name TEXT, status TEXT, effective_status TEXT,
          optimization_goal TEXT, destination_type TEXT, promoted_object TEXT, attribution_spec TEXT,
          PRIMARY KEY(account_id,adset_id)
        );
        CREATE TABLE IF NOT EXISTS ads(
          account_id TEXT, ad_id TEXT, campaign_id TEXT, adset_id TEXT, name TEXT, status TEXT,
          effective_status TEXT, created_time TEXT, creative_id TEXT, creative_name TEXT,
          creative_type TEXT, primary_text TEXT, headline TEXT, description TEXT, cta TEXT,
          page_id TEXT, form_id TEXT, story_id TEXT, thumbnail_url TEXT,
          PRIMARY KEY(account_id,ad_id)
        );
        CREATE TABLE IF NOT EXISTS lifetime(
          account_id TEXT, ad_id TEXT, spend REAL DEFAULT 0, impressions REAL DEFAULT 0, reach REAL DEFAULT 0,
          frequency REAL DEFAULT 0, clicks REAL DEFAULT 0, link_clicks REAL DEFAULT 0,
          cpc REAL DEFAULT 0, cpm REAL DEFAULT 0, ctr REAL DEFAULT 0,
          actions TEXT, cost_per_action_type TEXT, date_start TEXT, date_stop TEXT,
          PRIMARY KEY(account_id,ad_id)
        );
        CREATE TABLE IF NOT EXISTS daily(
          account_id TEXT, ad_id TEXT, day TEXT, spend REAL DEFAULT 0, leads REAL DEFAULT 0,
          lead_action_type TEXT, PRIMARY KEY(account_id,ad_id,day)
        );
        CREATE TABLE IF NOT EXISTS scan_progress(
          account_id TEXT PRIMARY KEY, phase TEXT NOT NULL DEFAULT 'campaigns', cursor TEXT,
          status TEXT NOT NULL DEFAULT 'pending', updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS errors(
          id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT, phase TEXT, error TEXT, created_at TEXT
        );
        """)
        self.db.commit()

    def set_meta(self, key: str, value: Any):
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, _json(value)))
        self.db.commit()

    def get_meta(self, key: str, default=None):
        r = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return _loads(r[0], default) if r else default

    def progress(self, account_id: str) -> tuple[str, str | None, str]:
        r = self.db.execute("SELECT phase,cursor,status FROM scan_progress WHERE account_id=?", (account_id,)).fetchone()
        if not r:
            self.db.execute("INSERT INTO scan_progress(account_id,phase,status,updated_at) VALUES(?,?,?,?)", (account_id, "campaigns", "pending", dt.datetime.now(dt.timezone.utc).isoformat()))
            self.db.commit()
            return "campaigns", None, "pending"
        return r["phase"], r["cursor"], r["status"]

    def set_progress(self, account_id: str, phase: str, cursor: str | None, status: str = "running"):
        self.db.execute("""INSERT INTO scan_progress(account_id,phase,cursor,status,updated_at) VALUES(?,?,?,?,?)
          ON CONFLICT(account_id) DO UPDATE SET phase=excluded.phase,cursor=excluded.cursor,status=excluded.status,updated_at=excluded.updated_at""",
          (account_id, phase, cursor, status, dt.datetime.now(dt.timezone.utc).isoformat()))
        self.db.commit()

    def error(self, aid: str, phase: str, message: str):
        self.db.execute("INSERT INTO errors(account_id,phase,error,created_at) VALUES(?,?,?,?)", (aid, phase, message[:1000], dt.datetime.now(dt.timezone.utc).isoformat()))
        self.db.execute("UPDATE accounts SET scan_status='partial',error=? WHERE account_id=?", (message[:700], aid))
        self.db.commit()

    def close(self):
        self.db.commit(); self.db.close()


class Budget:
    def __init__(self):
        try: minutes = int(os.getenv("HEAVY_RUN_BUDGET_MINUTES") or "300")
        except (TypeError, ValueError): minutes = 300
        minutes = max(5, min(315, minutes))
        self.deadline = time.monotonic() + minutes * 60

    def low(self, reserve_seconds: int = 420) -> bool:
        return time.monotonic() >= self.deadline - reserve_seconds


def _checkpoint(job_id: str, adb: AuditDB, reason: str, progress: dict):
    adb.set_meta("continuation_count", int(adb.get_meta("continuation_count", 0)) + 1)
    count = int(adb.get_meta("continuation_count", 0))
    try: max_cont = int(os.getenv("HEAVY_MAX_CONTINUATIONS") or "20")
    except (TypeError, ValueError): max_cont = 20
    if count > max_cont:
        raise RuntimeError("Heavy audit continuation safety limit reached")
    adb.set_meta("last_checkpoint_reason", reason)
    adb.db.commit()
    raw = Path(adb.path).read_bytes()
    packed = gzip.compress(raw, compresslevel=6)
    _gateway_raw(f"/internal/jobs/{job_id}/checkpoint", "POST", packed, "application/gzip", timeout=180)
    _gateway_json(f"/internal/jobs/{job_id}/progress", "POST", {"progress": progress, "continuation_count": count})
    _gateway_json(f"/internal/jobs/{job_id}/continue", "POST", {"reason": reason})
    raise ContinueRun()


def _load_checkpoint(job_id: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"elokaby_{job_id}_", suffix=".sqlite3")
    os.close(fd)
    try:
        packed = _gateway_raw(f"/internal/jobs/{job_id}/checkpoint", "GET", None, timeout=120)
        Path(path).write_bytes(gzip.decompress(packed))
    except RuntimeError as ex:
        if "HTTP 404" not in str(ex):
            raise
    return path


def _next_cursor(payload: dict, current: str | None) -> str | None:
    paging = payload.get("paging") if isinstance(payload, dict) else None
    if not isinstance(paging, dict) or not paging.get("next"):
        return None
    nxt = (paging.get("cursors") or {}).get("after")
    if not nxt or nxt == current:
        return None
    return str(nxt)


def _page_loop(job_id: str, adb: AuditDB, budget: Budget, client: MetaClient, aid: str, alias: str,
               phase: str, path: str, params: dict, consume):
    current_phase, cursor, _ = adb.progress(aid)
    if current_phase != phase:
        return
    seen = set()
    while True:
        if budget.low():
            _checkpoint(job_id, adb, "time_budget", {"account_id": aid, "phase": phase})
        try:
            payload, _ = client.request(path, alias, {**params, **({"after": cursor} if cursor else {})})
        except MetaError as ex:
            if "safety limit" in str(ex).lower():
                _checkpoint(job_id, adb, "meta_call_budget", {"account_id": aid, "phase": phase})
            raise
        for item in payload.get("data", []) or []:
            consume(item)
        adb.db.commit()
        nxt = _next_cursor(payload, cursor)
        if not nxt:
            break
        if nxt in seen:
            raise RuntimeError("Meta pagination cursor repeated")
        seen.add(nxt)
        cursor = nxt
        adb.set_progress(aid, phase, cursor)
    idx = PHASES.index(phase)
    adb.set_progress(aid, PHASES[idx + 1], None)


def _upsert_accounts(adb: AuditDB, discovery: dict, selected: list[str] | None):
    selected_set = set(selected or [])
    for a in discovery.get("accounts", []):
        aid = str(a.get("id") or "")
        if selected_set and aid not in selected_set:
            continue
        biz = a.get("business") if isinstance(a.get("business"), dict) else {}
        adb.db.execute("""INSERT INTO accounts(account_id,account_name,business_id,business_name,account_status,currency,timezone,token_alias,scan_status)
          VALUES(?,?,?,?,?,?,?,?,COALESCE((SELECT scan_status FROM accounts WHERE account_id=?),'pending'))
          ON CONFLICT(account_id) DO UPDATE SET account_name=excluded.account_name,business_id=excluded.business_id,business_name=excluded.business_name,
          account_status=excluded.account_status,currency=COALESCE(NULLIF(excluded.currency,''),accounts.currency),timezone=excluded.timezone,token_alias=excluded.token_alias""",
          (aid, a.get("name", ""), str((biz or {}).get("id") or a.get("business_id") or ""), str((biz or {}).get("name") or ""),
           str(a.get("account_status") or ""), str(a.get("currency") or ""), str(a.get("timezone_name") or ""), str(a.get("token_alias") or ""), aid))
        adb.progress(aid)
    adb.db.commit()


def _scan_account(job_id: str, adb: AuditDB, budget: Budget, client: MetaClient, account: sqlite3.Row, params: dict):
    aid, alias = account["account_id"], account["token_alias"]
    phase, _, status = adb.progress(aid)
    if status == "done" or phase == "done":
        return
    adb.db.execute("UPDATE accounts SET scan_status='running' WHERE account_id=?", (aid,)); adb.db.commit()

    # Best-effort account enrichment.
    try:
        detail, _ = client.request("/act_" + aid, alias, {"fields": "id,name,account_status,currency,timezone_name,business{id,name},created_time"})
        biz = detail.get("business") if isinstance(detail.get("business"), dict) else {}
        adb.db.execute("UPDATE accounts SET account_name=?,business_id=?,business_name=?,account_status=?,currency=?,timezone=? WHERE account_id=?",
            (detail.get("name", account["account_name"]), str((biz or {}).get("id") or account["business_id"] or ""), str((biz or {}).get("name") or account["business_name"] or ""),
             str(detail.get("account_status") or account["account_status"] or ""), str(detail.get("currency") or account["currency"] or ""), str(detail.get("timezone_name") or account["timezone"] or ""), aid))
        adb.db.commit()
    except Exception as ex:
        adb.error(aid, "account_details", str(ex))

    try:
        _page_loop(job_id, adb, budget, client, aid, alias, "campaigns", "/act_" + aid + "/campaigns",
                   {"fields": "id,name,status,effective_status,objective", "limit": 200},
                   lambda x: adb.db.execute("INSERT OR REPLACE INTO campaigns VALUES(?,?,?,?,?,?)", (aid, str(x.get("id") or ""), str(x.get("name") or ""), str(x.get("status") or ""), str(x.get("effective_status") or ""), str(x.get("objective") or ""))))
    except ContinueRun: raise
    except Exception as ex:
        adb.error(aid, "campaigns", str(ex)); adb.set_progress(aid, "adsets", None)

    try:
        _page_loop(job_id, adb, budget, client, aid, alias, "adsets", "/act_" + aid + "/adsets",
                   {"fields": "id,name,status,effective_status,optimization_goal,destination_type,promoted_object,attribution_spec,campaign_id", "limit": 200},
                   lambda x: adb.db.execute("INSERT OR REPLACE INTO adsets VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (aid, str(x.get("id") or ""), str(x.get("campaign_id") or ""), str(x.get("name") or ""), str(x.get("status") or ""), str(x.get("effective_status") or ""),
                      str(x.get("optimization_goal") or ""), str(x.get("destination_type") or ""), _json(x.get("promoted_object") or {}), _json(x.get("attribution_spec") or []))))
    except ContinueRun: raise
    except Exception as ex:
        adb.error(aid, "adsets", str(ex)); adb.set_progress(aid, "ads", None)

    def consume_ad(x):
        creative = x.get("creative") if isinstance(x.get("creative"), dict) else {}
        spec = creative.get("object_story_spec") if isinstance(creative.get("object_story_spec"), dict) else {}
        asset = creative.get("asset_feed_spec") if isinstance(creative.get("asset_feed_spec"), dict) else {}
        f = _creative_fields(spec or {}, asset or {})
        story = creative.get("effective_object_story_id") or creative.get("object_story_id") or ""
        adb.db.execute("""INSERT OR REPLACE INTO ads(account_id,ad_id,campaign_id,adset_id,name,status,effective_status,created_time,creative_id,creative_name,
          creative_type,primary_text,headline,description,cta,page_id,form_id,story_id,thumbnail_url) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (aid, str(x.get("id") or ""), str(x.get("campaign_id") or ""), str(x.get("adset_id") or ""), str(x.get("name") or ""), str(x.get("status") or ""),
           str(x.get("effective_status") or ""), str(x.get("created_time") or ""), str(creative.get("id") or ""), str(creative.get("name") or ""),
           f["creative_type"], f["primary_text"], f["headline"], f["description"], f["cta"], f["page_id"], f["form_id"], str(story), str(creative.get("thumbnail_url") or creative.get("image_url") or "")))
    try:
        _page_loop(job_id, adb, budget, client, aid, alias, "ads", "/act_" + aid + "/ads",
                   {"fields": "id,name,status,effective_status,created_time,campaign_id,adset_id,creative{id,name,thumbnail_url,image_url,object_story_id,effective_object_story_id,object_story_spec,asset_feed_spec}", "limit": 100}, consume_ad)
    except ContinueRun: raise
    except Exception as ex:
        adb.error(aid, "ads", str(ex)); adb.set_progress(aid, "lifetime", None)

    since, until = params.get("since"), params.get("until")
    datebits = {"time_range": _json({"since": since, "until": until})} if since and until else {"date_preset": "maximum"}
    insight_fields = "account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type"
    def consume_lifetime(x):
        adid = str(x.get("ad_id") or "")
        if not adid: return
        adb.db.execute("""INSERT OR REPLACE INTO lifetime(account_id,ad_id,spend,impressions,reach,frequency,clicks,link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,date_start,date_stop)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (aid, adid, _safe_float(x.get("spend")), _safe_float(x.get("impressions")), _safe_float(x.get("reach")), _safe_float(x.get("frequency")), _safe_float(x.get("clicks")),
           _safe_float(x.get("inline_link_clicks")), _safe_float(x.get("cpc")), _safe_float(x.get("cpm")), _safe_float(x.get("ctr")), _json(x.get("actions") or []),
           _json(x.get("cost_per_action_type") or []), str(x.get("date_start") or ""), str(x.get("date_stop") or "")))
    try:
        _page_loop(job_id, adb, budget, client, aid, alias, "lifetime", "/act_" + aid + "/insights",
                   {"level": "ad", "fields": insight_fields, "limit": 200, **datebits}, consume_lifetime)
    except ContinueRun: raise
    except Exception as ex:
        adb.error(aid, "lifetime", str(ex)); adb.set_progress(aid, "daily", None)

    if params.get("include_daily_consistency", True):
        lead_ids = set()
        for r in adb.db.execute("""SELECT a.ad_id,c.objective,s.optimization_goal,s.destination_type,l.actions
          FROM ads a LEFT JOIN campaigns c ON c.account_id=a.account_id AND c.campaign_id=a.campaign_id
          LEFT JOIN adsets s ON s.account_id=a.account_id AND s.adset_id=a.adset_id
          LEFT JOIN lifetime l ON l.account_id=a.account_id AND l.ad_id=a.ad_id WHERE a.account_id=?""", (aid,)):
            if _is_lead_candidate(r["objective"] or "", r["optimization_goal"] or "", r["destination_type"] or "", _loads(r["actions"], [])):
                lead_ids.add(str(r["ad_id"]))
        def consume_daily(x):
            adid = str(x.get("ad_id") or "")
            if adid not in lead_ids: return
            leads, typ, _ = extract_leads(x.get("actions") or [])
            day = str(x.get("date_start") or x.get("date_stop") or "")
            if day:
                adb.db.execute("INSERT OR REPLACE INTO daily(account_id,ad_id,day,spend,leads,lead_action_type) VALUES(?,?,?,?,?,?)",
                               (aid, adid, day, _safe_float(x.get("spend")), leads, typ or ""))
        try:
            _page_loop(job_id, adb, budget, client, aid, alias, "daily", "/act_" + aid + "/insights",
                       {"level": "ad", "fields": "ad_id,spend,actions", "time_increment": 1, "limit": 200, **datebits}, consume_daily)
        except ContinueRun: raise
        except Exception as ex:
            adb.error(aid, "daily", str(ex)); adb.set_progress(aid, "finalize", None)
    elif adb.progress(aid)[0] == "daily":
        adb.set_progress(aid, "finalize", None)

    if adb.progress(aid)[0] == "finalize":
        total = adb.db.execute("SELECT COUNT(*) FROM ads WHERE account_id=?", (aid,)).fetchone()[0]
        lead_total = 0
        for r in adb.db.execute("""SELECT a.ad_id,c.objective,s.optimization_goal,s.destination_type,l.actions
          FROM ads a LEFT JOIN campaigns c ON c.account_id=a.account_id AND c.campaign_id=a.campaign_id
          LEFT JOIN adsets s ON s.account_id=a.account_id AND s.adset_id=a.adset_id
          LEFT JOIN lifetime l ON l.account_id=a.account_id AND l.ad_id=a.ad_id WHERE a.account_id=?""", (aid,)):
            if _is_lead_candidate(r["objective"] or "", r["optimization_goal"] or "", r["destination_type"] or "", _loads(r["actions"], [])):
                lead_total += 1
        date_row = adb.db.execute("SELECT MIN(day),MAX(day) FROM daily WHERE account_id=?", (aid,)).fetchone()
        existing = adb.db.execute("SELECT error FROM accounts WHERE account_id=?", (aid,)).fetchone()
        final_status = "partial" if existing and existing[0] else "complete"
        adb.db.execute("UPDATE accounts SET scan_status=?,total_ads=?,lead_ads=?,earliest_date=?,latest_date=? WHERE account_id=?",
                       (final_status, total, lead_total, date_row[0] or "", date_row[1] or "", aid))
        adb.set_progress(aid, "done", None, "done")
        adb.db.commit()


def _daily_stats(adb: AuditDB) -> dict[tuple[str, str], dict]:
    out = {}
    current = None
    values = []
    for r in adb.db.execute("SELECT account_id,ad_id,day,spend,leads FROM daily ORDER BY account_id,ad_id,day"):
        key = (r["account_id"], r["ad_id"])
        if current is not None and key != current:
            out[current] = _stats(values); values = []
        current = key; values.append(dict(r))
    if current is not None:
        out[current] = _stats(values)
    return out


def _stats(values: list[dict]) -> dict:
    if not values:
        return {"delivery_days": 0, "lead_days": 0, "first": "", "last": "", "consistency": "Unavailable", "cpl_cv": None}
    active = [x for x in values if _safe_float(x["spend"]) > 0 or _safe_float(x["leads"]) > 0]
    cpls = [_safe_float(x["spend"]) / _safe_float(x["leads"]) for x in active if _safe_float(x["leads"]) > 0]
    cv = None
    if len(cpls) >= 2 and statistics.mean(cpls) > 0:
        cv = statistics.pstdev(cpls) / statistics.mean(cpls)
    consistency = "Unavailable" if not cpls else ("Stable" if (cv is None or cv <= .35) else "Moderate" if cv <= .70 else "Volatile")
    return {"delivery_days": len(active), "lead_days": sum(1 for x in active if _safe_float(x["leads"]) > 0),
            "first": active[0]["day"] if active else "", "last": active[-1]["day"] if active else "", "consistency": consistency, "cpl_cv": cv}


def _records(adb: AuditDB, params: dict) -> tuple[list[dict], dict]:
    ds = _daily_stats(adb)
    raw = []
    q = """SELECT acc.account_name,acc.account_id,acc.business_name,acc.business_id,acc.account_status,acc.currency,acc.timezone,
      a.ad_id,a.name ad_name,a.status ad_status,a.effective_status ad_effective_status,a.created_time,a.creative_id,a.creative_type,a.primary_text,a.headline,a.description,a.cta,a.page_id,a.form_id,a.story_id,
      c.campaign_id,c.name campaign_name,c.status campaign_status,c.objective,
      s.adset_id,s.name adset_name,s.status adset_status,s.optimization_goal,s.destination_type,s.attribution_spec,
      l.spend,l.impressions,l.reach,l.frequency,l.clicks,l.link_clicks,l.cpc,l.cpm,l.ctr,l.actions,l.cost_per_action_type,l.date_start,l.date_stop
      FROM ads a JOIN accounts acc ON acc.account_id=a.account_id
      LEFT JOIN campaigns c ON c.account_id=a.account_id AND c.campaign_id=a.campaign_id
      LEFT JOIN adsets s ON s.account_id=a.account_id AND s.adset_id=a.adset_id
      LEFT JOIN lifetime l ON l.account_id=a.account_id AND l.ad_id=a.ad_id"""
    for r in adb.db.execute(q):
        actions = _loads(r["actions"], [])
        if not _is_lead_candidate(r["objective"] or "", r["optimization_goal"] or "", r["destination_type"] or "", actions):
            continue
        leads, action_type, lead_actions = extract_leads(actions)
        spend = _safe_float(r["spend"])
        link_clicks = _safe_float(r["link_clicks"]) or _safe_float(r["clicks"])
        cpl = spend / leads if leads > 0 else None
        conv = _conversion_type(r["objective"] or "", r["optimization_goal"] or "", r["destination_type"] or "", action_type)
        stat = ds.get((r["account_id"], r["ad_id"]), _stats([]))
        raw.append({
          "Business Name": r["business_name"] or "", "Business ID": r["business_id"] or "", "Ad Account Name": r["account_name"] or "", "Ad Account ID": r["account_id"],
          "Currency": r["currency"] or "", "Campaign Name": r["campaign_name"] or "", "Campaign ID": r["campaign_id"] or "", "Campaign Objective": r["objective"] or "",
          "Ad Set Name": r["adset_name"] or "", "Ad Set ID": r["adset_id"] or "", "Ad Name": r["ad_name"] or "", "Ad ID": r["ad_id"], "Ad Status": r["ad_effective_status"] or r["ad_status"] or "",
          "Creative Type": r["creative_type"] or "", "Created Date": r["created_time"] or "", "First Delivery Date": stat["first"] or r["date_start"] or "", "Last Delivery Date": stat["last"] or r["date_stop"] or "",
          "Spend": spend, "Leads": leads, "CPL": cpl, "Impressions": _safe_float(r["impressions"]), "Reach": _safe_float(r["reach"]), "Frequency": _safe_float(r["frequency"]),
          "CPM": _safe_float(r["cpm"]), "Link Clicks": _safe_float(r["link_clicks"]), "CPC": _safe_float(r["cpc"]), "CTR": _safe_float(r["ctr"]),
          "Conversion Rate": (leads / link_clicks) if link_clicks > 0 else None, "Conversion Type": conv, "Lead Action Type": action_type or "", "Optimization Goal": r["optimization_goal"] or "",
          "Attribution Setting": r["attribution_spec"] or "", "Primary Text": r["primary_text"] or "", "Headline": r["headline"] or "", "Page Name": "", "Page ID": r["page_id"] or "", "Form Name": "", "Form ID": r["form_id"] or "",
          "Delivery Days": stat["delivery_days"], "Lead Days": stat["lead_days"], "Historical Performance Consistency": stat["consistency"], "Daily CPL CV": stat["cpl_cv"],
          "Raw Lead Actions": _json(lead_actions), "Ad Link": _ad_link(r["account_id"], r["ad_id"]), "Original Post Link": _post_link(r["story_id"]),
        })
    # Weighted benchmark by account + conversion type.
    buckets = defaultdict(lambda: {"spend": 0.0, "leads": 0.0})
    for x in raw:
        k = (x["Ad Account ID"], x["Conversion Type"]); buckets[k]["spend"] += x["Spend"]; buckets[k]["leads"] += x["Leads"]
    for x in raw:
        b = buckets[(x["Ad Account ID"], x["Conversion Type"])]
        bench = b["spend"] / b["leads"] if b["leads"] > 0 else None
        x["Account Benchmark CPL"] = bench
        x["CPL Improvement vs Benchmark"] = ((bench - x["CPL"]) / bench) if bench and x["CPL"] is not None else None
        _classify(x, params)
    return raw, {"benchmarks": {str(k): v for k, v in buckets.items()}}


def _classify(x: dict, params: dict):
    leads, spend, cpl, bench = x["Leads"], x["Spend"], x["CPL"], x["Account Benchmark CPL"]
    days = x["Delivery Days"]
    min_winner = max(3, int(params.get("min_winner_leads", 20)))
    min_potential = max(1, int(params.get("min_potential_leads", 5)))
    target = params.get("target_cpl")
    effective = float(target) if target not in (None, "") else bench
    stable = x["Historical Performance Consistency"] in ("Stable", "Moderate")
    if cpl is not None and effective and leads >= min_winner and cpl <= effective * .90 and (days >= 5 or x["Historical Performance Consistency"] == "Unavailable") and stable:
        cls = "WINNER"; conf = "High" if leads >= min_winner * 2 and days >= 10 else "Medium"
        reason = f"{leads:g} leads at {cpl:.2f} CPL versus {effective:.2f} benchmark/target, with {x['Historical Performance Consistency'].lower()} historical delivery."
        action = "Controlled Scaling / Reuse Creative"
    elif cpl is not None and effective and leads >= min_potential and cpl <= effective * 1.05:
        cls = "POTENTIAL WINNER"; conf = "Medium" if leads >= min_potential * 2 else "Low"
        reason = f"Promising efficiency: {leads:g} leads at {cpl:.2f} CPL versus {effective:.2f}, but evidence is below the proven Winner threshold or consistency requirement."
        action = "Retest / Increase Testing Budget"
    elif effective and spend >= effective * max(3, min_potential) and (leads == 0 or (cpl is not None and cpl > effective * 1.25)):
        cls = "UNDERPERFORMER"; conf = "High" if spend >= effective * max(6, min_winner / 2) else "Medium"
        reason = "Sufficient spend evidence with weak lead acquisition efficiency versus the relevant account/conversion benchmark."
        action = "Investigate Performance / No Further Testing"
    else:
        cls = "INSUFFICIENT DATA"; conf = "Low"
        reason = "Insufficient lead volume, spend, delivery history, or benchmark evidence for a reliable classification."
        action = "Collect More Data / Retest"
    x["Classification"] = cls; x["Confidence Level"] = conf; x["Classification Reason"] = reason; x["Recommended Next Action"] = action


def _xlsx(adb: AuditDB, records: list[dict], extracted_at: str) -> bytes:
    import xlsxwriter
    b = io.BytesIO(); wb = xlsxwriter.Workbook(b, {"in_memory": True})
    fmt_header = wb.add_format({"bold": True, "bg_color": "#17365D", "font_color": "#FFFFFF", "text_wrap": True, "valign": "vcenter"})
    fmt_money = wb.add_format({"num_format": "#,##0.00"}); fmt_pct = wb.add_format({"num_format": "0.00%"}); fmt_int = wb.add_format({"num_format": "#,##0"})
    fmt_win = wb.add_format({"bg_color": "#E2F0D9"}); fmt_pot = wb.add_format({"bg_color": "#FFF2CC"}); fmt_bad = wb.add_format({"bg_color": "#FCE4D6"}); fmt_low = wb.add_format({"bg_color": "#EDEDED"})
    master_cols = ["Business Name","Business ID","Ad Account Name","Ad Account ID","Currency","Campaign Name","Campaign ID","Campaign Objective","Ad Set Name","Ad Set ID","Ad Name","Ad ID","Ad Status","Creative Type","Created Date","First Delivery Date","Last Delivery Date","Spend","Leads","CPL","Impressions","Reach","Frequency","CPM","Link Clicks","CPC","CTR","Conversion Rate","Conversion Type","Lead Action Type","Optimization Goal","Attribution Setting","Primary Text","Headline","Page Name","Page ID","Form Name","Form ID","Delivery Days","Lead Days","Historical Performance Consistency","Daily CPL CV","Classification","Confidence Level","Classification Reason","Account Benchmark CPL","CPL Improvement vs Benchmark","Recommended Next Action","Ad Link","Original Post Link"]

    def write_sheet(name: str, rows: list[dict], cols: list[str] = master_cols):
        ws = wb.add_worksheet(name); ws.freeze_panes(1, 0); ws.autofilter(0, 0, max(1, len(rows)), len(cols)-1)
        for j, c in enumerate(cols): ws.write(0, j, c, fmt_header); ws.set_column(j, j, 16 if c.endswith("ID") else 22)
        long_cols = {"Primary Text","Classification Reason","Recommended Next Action","Attribution Setting"}
        for j, c in enumerate(cols):
            if c in long_cols: ws.set_column(j, j, 42)
            if c in ("Ad Link","Original Post Link"): ws.set_column(j, j, 38)
        for i, r in enumerate(rows, 1):
            for j, c in enumerate(cols):
                v = r.get(c)
                if c in ("Ad Link","Original Post Link") and isinstance(v, str) and v.startswith("https://"):
                    ws.write_url(i, j, v, string=v)
                elif c in ("Spend","CPL","CPM","CPC","Account Benchmark CPL") and isinstance(v, (int,float)):
                    ws.write_number(i, j, v, fmt_money)
                elif c in ("CTR","Conversion Rate","Daily CPL CV","CPL Improvement vs Benchmark") and isinstance(v, (int,float)):
                    vv = v/100 if c == "CTR" else v
                    ws.write_number(i, j, vv, fmt_pct)
                elif c in ("Leads","Impressions","Reach","Link Clicks","Delivery Days","Lead Days") and isinstance(v, (int,float)):
                    ws.write_number(i, j, v, fmt_int)
                elif v is None:
                    ws.write_blank(i, j, None)
                else:
                    s = str(v)
                    if s.lstrip()[:1] in ("=", "+", "-", "@") or s[:1] in ("\t", "\r", "\n"): s = "'" + s
                    ws.write_string(i, j, s[:32000])
        if "Classification" in cols and rows:
            c = cols.index("Classification")
            ws.conditional_format(1, c, len(rows), c, {"type": "text", "criteria": "containing", "value": "WINNER", "format": fmt_win})
            ws.conditional_format(1, c, len(rows), c, {"type": "text", "criteria": "containing", "value": "POTENTIAL", "format": fmt_pot})
            ws.conditional_format(1, c, len(rows), c, {"type": "text", "criteria": "containing", "value": "UNDERPERFORMER", "format": fmt_bad})
            ws.conditional_format(1, c, len(rows), c, {"type": "text", "criteria": "containing", "value": "INSUFFICIENT", "format": fmt_low})
        return ws

    write_sheet("ALL LEAD GEN ADS", records)
    winner_cols = master_cols + []
    write_sheet("WINNERS", [r for r in records if r["Classification"] == "WINNER"], winner_cols)
    write_sheet("POTENTIAL WINNERS", [r for r in records if r["Classification"] == "POTENTIAL WINNER"], winner_cols)

    summary = wb.add_worksheet("ANALYSIS SUMMARY"); summary.set_column(0,0,34); summary.set_column(1,1,24); summary.set_column(3,11,18)
    summary.write(0,0,"El Okaby Historical Lead Generation Audit",fmt_header)
    summary.write(1,0,"Extraction timestamp UTC"); summary.write(1,1,extracted_at)
    accounts = list(adb.db.execute("SELECT * FROM accounts ORDER BY account_id"))
    completed = sum(1 for a in accounts if a["scan_status"] == "complete")
    partial = sum(1 for a in accounts if a["scan_status"] == "partial")
    failed = sum(1 for a in accounts if a["scan_status"] == "failed")
    classes = defaultdict(int)
    for r in records: classes[r["Classification"]] += 1
    metrics = [
      ("Total Ad Accounts Discovered",len(accounts)),("Successfully Scanned Accounts",completed),("Partially Scanned Accounts",partial),("Failed Accounts",failed),
      ("Total Ads Discovered",sum(a["total_ads"] or 0 for a in accounts)),("Total Lead Generation Ads",len(records)),("Winners",classes["WINNER"]),("Potential Winners",classes["POTENTIAL WINNER"]),
      ("Underperformers",classes["UNDERPERFORMER"]),("Insufficient Data",classes["INSUFFICIENT DATA"]),("Historical Scan Status","COMPLETE" if accounts and completed==len(accounts) else "PARTIAL")]
    for i,(k,v) in enumerate(metrics,3): summary.write(i,0,k); summary.write(i,1,v)
    summary.write(3,3,"Financial Summary by Currency",fmt_header)
    by_cur = defaultdict(lambda:{"spend":0.0,"leads":0.0,"winner_spend":0.0,"winner_leads":0.0,"potential_spend":0.0,"potential_leads":0.0})
    for r in records:
        d=by_cur[r["Currency"] or "Unknown"]; d["spend"]+=r["Spend"]; d["leads"]+=r["Leads"]
        if r["Classification"]=="WINNER": d["winner_spend"]+=r["Spend"]; d["winner_leads"]+=r["Leads"]
        if r["Classification"]=="POTENTIAL WINNER": d["potential_spend"]+=r["Spend"]; d["potential_leads"]+=r["Leads"]
    headers=["Currency","Spend","Leads","Weighted CPL","Winner Spend","Winner Leads","Winner CPL","Potential Spend","Potential Leads","Potential CPL"]
    for j,h in enumerate(headers): summary.write(4,j+3,h,fmt_header)
    for i,(cur,d) in enumerate(sorted(by_cur.items()),5):
        vals=[cur,d["spend"],d["leads"],d["spend"]/d["leads"] if d["leads"] else None,d["winner_spend"],d["winner_leads"],d["winner_spend"]/d["winner_leads"] if d["winner_leads"] else None,d["potential_spend"],d["potential_leads"],d["potential_spend"]/d["potential_leads"] if d["potential_leads"] else None]
        for j,v in enumerate(vals):
            if isinstance(v,(int,float)): summary.write_number(i,j+3,v,fmt_money if j in (1,3,4,6,7,9) else fmt_int)
            elif v is None: summary.write_blank(i,j+3,None)
            else: summary.write(i,j+3,v)
    start = 5 + len(by_cur) + 2
    summary.write(start,3,"Account-Level Summary",fmt_header)
    ah=["Ad Account","Account ID","Total Ads","Lead Gen Ads","Winners","Potential","Spend","Leads","Weighted CPL","Currency","Scan Status"]
    for j,h in enumerate(ah): summary.write(start+1,j+3,h,fmt_header)
    by_account = defaultdict(list)
    for r in records: by_account[r["Ad Account ID"]].append(r)
    for i,a in enumerate(accounts,start+2):
        rr=by_account[a["account_id"]]; spend=sum(x["Spend"] for x in rr); leads=sum(x["Leads"] for x in rr)
        vals=[a["account_name"],a["account_id"],a["total_ads"],len(rr),sum(x["Classification"]=="WINNER" for x in rr),sum(x["Classification"]=="POTENTIAL WINNER" for x in rr),spend,leads,spend/leads if leads else None,a["currency"],a["scan_status"]]
        for j,v in enumerate(vals):
            if isinstance(v,(int,float)): summary.write_number(i,j+3,v,fmt_money if j in (6,8) else fmt_int)
            elif v is None: summary.write_blank(i,j+3,None)
            else: summary.write(i,j+3,str(v or ""))

    cov = wb.add_worksheet("SCAN COVERAGE & ERRORS"); cov.set_column(0,15,22)
    cov_cols=["Account Name","Account ID","Business ID","Account Status","Currency","Scan Status","Earliest Available Date","Latest Scanned Date","Total Ads Retrieved","Lead Generation Ads Retrieved","API Errors","Data Completeness"]
    for j,h in enumerate(cov_cols): cov.write(0,j,h,fmt_header)
    errors_by=defaultdict(list)
    for e in adb.db.execute("SELECT account_id,phase,error FROM errors ORDER BY id"):
        errors_by[e["account_id"]].append(f"{e['phase']}: {e['error']}")
    for i,a in enumerate(accounts,1):
        vals=[a["account_name"],a["account_id"],a["business_id"],a["account_status"],a["currency"],a["scan_status"],a["earliest_date"],a["latest_date"],a["total_ads"],a["lead_ads"]," | ".join(errors_by[a["account_id"]])[:32000],"Complete" if a["scan_status"]=="complete" else "Partial"]
        for j,v in enumerate(vals): cov.write(i,j,v if v is not None else "")
    cov.freeze_panes(1,0); cov.autofilter(0,0,max(1,len(accounts)),len(cov_cols)-1)

    wb.close(); return b.getvalue()


def run_historical_audit(job: dict, client: MetaClient) -> tuple[dict | None, bytes | None, str | None, bool]:
    """Return (result, report_bytes, mime, continued).

    continued=True means the runner should exit successfully without marking the job complete;
    the gateway has already queued the next continuation for the same logical job ID.
    """
    job_id = job["job_id"]; params = job["input"].get("params") or {}
    path = _load_checkpoint(job_id); adb = AuditDB(path); budget = Budget()
    try:
        discovery = client.discover()
        selected = [clean_account(x) for x in params.get("account_ids", [])] if params.get("account_ids") else None
        _upsert_accounts(adb, discovery, selected)
        if selected:
            found = {r[0] for r in adb.db.execute("SELECT account_id FROM accounts")}
            missing = [x for x in selected if x not in found]
            for x in missing:
                adb.db.execute("INSERT OR IGNORE INTO accounts(account_id,scan_status,error) VALUES(?,?,?)", (x,"failed","Account not accessible through configured tokens"))
            adb.db.commit()
        accounts = list(adb.db.execute("SELECT * FROM accounts ORDER BY account_id"))
        if not accounts:
            raise RuntimeError("No accessible Meta ad accounts discovered")
        total = len(accounts)
        for idx, account in enumerate(accounts, 1):
            phase, _, status = adb.progress(account["account_id"])
            if status == "done" or phase == "done":
                continue
            _gateway_json(f"/internal/jobs/{job_id}/progress", "POST", {"progress":{"account": idx,"total_accounts": total,"account_id":account["account_id"],"phase":phase}})
            _scan_account(job_id, adb, budget, client, account, params)
            # Durable checkpoint after every completed account. If the runner later dies, at most one account is repeated.
            raw = Path(adb.path).read_bytes(); packed = gzip.compress(raw, compresslevel=4)
            _gateway_raw(f"/internal/jobs/{job_id}/checkpoint", "POST", packed, "application/gzip", timeout=180)
            if budget.low():
                _checkpoint(job_id, adb, "time_budget_after_account", {"account": idx,"total_accounts": total,"account_id":account["account_id"],"phase":"done"})
        extracted = dt.datetime.now(dt.timezone.utc).isoformat()
        records, _ = _records(adb, params)
        report = _xlsx(adb, records, extracted)
        accounts = list(adb.db.execute("SELECT scan_status FROM accounts"))
        complete = bool(accounts) and all(a[0] == "complete" for a in accounts)
        classes = defaultdict(int)
        for r in records: classes[r["Classification"]] += 1
        result = {
          "audit_status": "COMPLETE" if complete else "PARTIAL",
          "ad_accounts_discovered": len(accounts),
          "accounts_successfully_scanned": sum(a[0] == "complete" for a in accounts),
          "lead_generation_ads_analyzed": len(records),
          "winners": classes["WINNER"], "potential_winners": classes["POTENTIAL WINNER"],
          "underperformers": classes["UNDERPERFORMER"], "insufficient_data": classes["INSUFFICIENT DATA"],
          "extracted_at_utc": extracted,
          "report_filename": "El_Okaby_Historical_LeadGen_Winners_Analysis.xlsx",
          "note": "Monetary totals are kept by account currency in the workbook; currencies are not silently combined."
        }
        return result, report, MIME_XLSX, False
    except ContinueRun:
        return None, None, None, True
    finally:
        adb.close()
        try: os.remove(path)
        except OSError: pass
