"""Single-job analysis over a persisted El Okaby parent dataset.

No Meta API calls are made here. ChatGPT supplies the analysis policy once (encoded through
an existing export_job_data request), then this module reads the stored parent R2/SQLite
artifacts, performs the heavy joins/aggregations/daily consistency work in one GitHub Action,
and produces the final XLSX plus a compact JSON summary.
"""
from __future__ import annotations

import json
import gzip
import hashlib
import datetime as dt
import urllib.parse
import math
import os
import re
import sqlite3
import statistics
import tempfile
from collections import Counter, defaultdict
from pathlib import Path

import xlsxwriter

from .dataset_tools import _iter_rows, _load_parent, _manifest
from .generic_job import gateway_json, gateway_raw

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _progress(job_id: str, phase: str, **extra):
    try:
        gateway_json(
            f"/internal/jobs/{job_id}/progress",
            "POST",
            {"progress": {"phase": phase, **extra}, "continuation_count": 0},
        )
    except Exception:
        pass


def _f(v):
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _s(v):
    return "" if v is None else str(v)


def _actions(row):
    out = defaultdict(float)
    for a in (row or {}).get("actions") or []:
        t = _s(a.get("action_type"))
        if t:
            out[t] += _f(a.get("value"))
    return dict(out)


def _cv(vals):
    vals = [x for x in vals if x is not None and math.isfinite(x)]
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    return statistics.stdev(vals) / m if m else None


def _quantile(vals, p):
    a = sorted(x for x in vals if x is not None and math.isfinite(x))
    if not a:
        return None
    if len(a) == 1:
        return a[0]
    k = (len(a) - 1) * p
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    if lo == hi:
        return a[lo]
    return a[lo] * (hi - k) + a[hi] * (k - lo)


def _nested(obj, keys):
    wanted = {str(k).lower() for k in keys}
    stack = [obj]
    while stack:
        x = stack.pop()
        if isinstance(x, dict):
            for k, v in x.items():
                if str(k).lower() in wanted and v not in (None, "", [], {}):
                    return v
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(x, list):
            stack.extend(x)
    return None


def _defaults(policy):
    p = dict(policy or {})
    defaults = {
        "agent_map": {
            "AA": "Abdallah", "HM": "Hesham", "BM": "Bassem", "AF": "Amr",
            "EK": "Esraa", "MA": "Mahmoud", "MM": "Mohamed", "OS": "Osama",
            "SQ": "Sharkawy", "NB": "Nabih", "AD": "Adham", "SH": "Sherif",
        },
        "lead_actions": ["lead", "onsite_conversion.lead_grouped", "offsite_conversion.fb_pixel_lead"],
        "messaging_actions": [
            "onsite_conversion.messaging_conversation_started_7d",
            "onsite_conversion.messaging_first_reply",
            "onsite_conversion.total_messaging_connection",
        ],
        "conversion_actions": [
            "purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase",
            "complete_registration", "offsite_conversion.fb_pixel_complete_registration",
        ],
        "benchmark_min_ads": 8,
        "benchmark_min_results": 20,
        "winner_cost_ratio": 0.80,
        "winner_high_volume_cost_ratio": 0.90,
        "winner_min_results": 5,
        "winner_min_active_days": 3,
        "winner_min_result_days": 3,
        "winner_max_daily_cv": 1.25,
        "winner_max_zero_result_spend_ratio": 0.45,
        "potential_cost_ratio": 0.95,
        "potential_min_results": 2,
        "underperform_cost_ratio": 1.25,
        "underperform_spend_multiple": 3.0,
        "zero_result_spend_multiple": 2.0,
        "trend_deterioration_ratio": 1.35,
        "trend_improvement_ratio": 0.75,
        "report_title": "El Okaby Full Historical Multi-Objective Audit",
    }
    for k, v in defaults.items():
        p.setdefault(k, v)
    return p


def _agent(name, p):
    text = _s(name).upper()
    for code, full in p["agent_map"].items():
        code_u = code.upper()
        if re.search(r"(^|[^A-Z0-9])" + re.escape(code_u) + r"(?=[^A-Z0-9]|$)", text):
            confidence = "High" if re.match(r"^\s*" + re.escape(code_u) + r"(?=[^A-Z0-9]|$)", text) else "Medium"
            return code, full, confidence
    return "", "Unmapped", "Unmapped"


def _name_code(name):
    u = _s(name).upper()
    tests = [
        ("WhatsApp", r"(^|[-_\s])WA(?=[-_\s]|$)"),
        ("Lead Messaging", r"(^|[-_\s])LM(?=[-_\s]|$)"),
        ("Lead Generation", r"(^|[-_\s])LG(?=[-_\s]|$)"),
        ("Conversion", r"(^|[-_\s])CONV(?:S|L)?(?=[-_\s]|$)"),
    ]
    for label, pat in tests:
        if re.search(pat, u):
            return label
    return ""


def _select_action(family, acts, p):
    choices = (
        p["messaging_actions"] if family in ("WhatsApp", "Lead Messaging")
        else p["lead_actions"] if family == "Lead Generation"
        else p["conversion_actions"] if family == "Conversion"
        else []
    )
    for t in choices:
        if t in acts:
            return t
    if family == "Lead Generation":
        for t in acts:
            if "lead" in t.lower() and "messaging" not in t.lower():
                return t
    return choices[0] if choices else ""


def _objective(campaign, adset, creative, acts, p):
    code = _name_code(campaign.get("name"))
    objective = _s(campaign.get("objective")).upper()
    dest = _s(adset.get("destination_type")).upper()
    opt = _s(adset.get("optimization_goal")).upper()
    promoted = adset.get("promoted_object") or {}
    cta = _s((creative or {}).get("call_to_action_type")).upper()
    keys = set(acts)
    has_msg = any("messaging_" in x or "messaging." in x for x in keys)
    has_lead = any("lead" in x.lower() and "messaging" not in x.lower() for x in keys)
    has_conv = any(x in keys for x in p["conversion_actions"])
    whatsapp = "WHATSAPP" in dest or bool(promoted.get("whatsapp_phone_number")) or "WHATSAPP" in cta

    if whatsapp and (has_msg or opt == "CONVERSATIONS" or code == "WhatsApp"):
        return "WhatsApp", "High", "WhatsApp destination/configuration plus messaging behavior", code
    if (("MESSENGER" in dest or "INSTAGRAM_DIRECT" in dest) and not whatsapp) and (has_msg or opt == "CONVERSATIONS" or code == "Lead Messaging"):
        return "Lead Messaging", "High" if has_msg else "Medium", f"destination={dest}; optimization={opt}", code
    if has_conv and not has_lead:
        return "Conversion", "High", "actual conversion action behavior", code
    if has_lead and not whatsapp:
        return "Lead Generation", "High", "actual lead action behavior", code
    if code == "Conversion":
        return "Conversion", "Medium", "campaign code + Meta configuration fallback", code
    if code == "Lead Messaging":
        return "Lead Messaging", "Medium", "campaign code + Meta configuration fallback", code
    if code == "Lead Generation":
        return "Lead Generation", "Medium", "campaign code + Meta configuration fallback", code
    if code == "WhatsApp":
        return "WhatsApp", "Medium", "campaign code fallback", code
    if objective in ("OUTCOME_LEADS", "LEAD_GENERATION") and opt in ("LEAD_GENERATION", "QUALITY_LEAD") and not whatsapp:
        return "Lead Generation", "Medium", f"objective={objective}; optimization={opt}", code
    if objective == "OUTCOME_SALES" or opt == "OFFSITE_CONVERSIONS":
        return "Conversion", "Medium", f"objective={objective}; optimization={opt}", code
    return "Other / Unclassified", "Low", "insufficient evidence", code


def _bench(rows):
    good = [r for r in rows if r["results"] > 0 and r["spend"] > 0]
    spend = sum(r["spend"] for r in good)
    results = sum(r["results"] for r in good)
    return {
        "n": len(good),
        "spend": spend,
        "results": results,
        "weighted_cost": spend / results if results else None,
        "p50_results": _quantile([r["results"] for r in good], 0.5),
        "p75_results": _quantile([r["results"] for r in good], 0.75),
    }


def _pick_benchmark(r, pools, p):
    candidates = [
        ("Account+Destination+Event", (r["currency"], r["objective_family"], r["primary_action"], r["destination_type"], r["account_id"])),
        ("Destination+Event", (r["currency"], r["objective_family"], r["primary_action"], r["destination_type"])),
        ("Objective+Event", (r["currency"], r["objective_family"], r["primary_action"])),
        ("Objective", (r["currency"], r["objective_family"])),
    ]
    for scope, key in candidates:
        b = pools.get((scope, key))
        if b and b["n"] >= p["benchmark_min_ads"] and b["results"] >= p["benchmark_min_results"] and b["weighted_cost"]:
            return scope, b
    for scope, key in candidates:
        b = pools.get((scope, key))
        if b and b["n"] >= 2 and b["weighted_cost"]:
            return scope + " (small sample)", b
    return "Unavailable", {"n": 0, "weighted_cost": None, "p50_results": None, "p75_results": None}


def _classify(r, b, p):
    bc = b.get("weighted_cost")
    if r["objective_family"] == "Other / Unclassified" or not r["primary_action"]:
        return "Insufficient Data", "Low", "Objective/result event could not be validated", "Investigate configuration"
    if not bc:
        if r["results"] >= p["winner_min_results"] and r["active_days"] >= p["winner_min_active_days"]:
            return "Potential Winner", "Low", "Promising evidence but no reliable comparable benchmark", "Retest / build benchmark"
        return "Insufficient Data", "Low", "No reliable comparable benchmark", "Increase testing data"
    ratio = r["cost_per_result"] / bc if r["cost_per_result"] is not None else None
    if r["results"] == 0:
        if r["spend"] >= bc * p["zero_result_spend_multiple"]:
            return "Underperformer", "High", "Zero primary results despite material spend", "No Further Testing / investigate"
        return "Insufficient Data", "Medium", "No primary results and spend below decisive threshold", "Increase Testing Budget"
    need = max(p["winner_min_results"], min(20, int(math.ceil(b.get("p50_results") or 0))))
    stable = (r["daily_cost_cv"] is None or r["daily_cost_cv"] <= p["winner_max_daily_cv"]) and (
        r["zero_result_spend_ratio"] is None or r["zero_result_spend_ratio"] <= p["winner_max_zero_result_spend_ratio"]
    )
    if ratio <= p["winner_cost_ratio"] and r["results"] >= need and r["active_days"] >= p["winner_min_active_days"] and r["result_days"] >= p["winner_min_result_days"] and stable:
        return "Winner", "High", f"Cost {ratio:.0%} of benchmark with meaningful volume and daily consistency", "Reuse Creative / Controlled Scaling"
    if ratio <= p["winner_high_volume_cost_ratio"] and r["results"] >= max(need, b.get("p75_results") or 0) and r["active_days"] >= p["winner_min_active_days"]:
        return "Winner", "High", f"High volume with cost {ratio:.0%} of benchmark", "Controlled Scaling / Monitor Fatigue"
    if ratio <= p["potential_cost_ratio"] and r["results"] >= p["potential_min_results"]:
        return "Potential Winner", "Medium", f"Cost {ratio:.0%} of benchmark; evidence is promising but not yet Winner-grade", "Retest / controlled budget increase"
    if ratio >= p["underperform_cost_ratio"] and r["spend"] >= bc * p["underperform_spend_multiple"]:
        return "Underperformer", "High", f"Cost {ratio:.0%} of benchmark after material spend", "Investigate Performance / No Further Testing"
    if r["results"] >= max(3, p["potential_min_results"]) and ratio > 1.10:
        return "Underperformer", "Medium", f"Cost {ratio:.0%} of benchmark with sufficient result sample", "Investigate Performance"
    return "Insufficient Data", "Medium", "Evidence is not strong enough for a reliable classification", "Retest / monitor"


def _safe(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        v = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    s = str(v)
    if s[:1] in ("=", "+", "-", "@", "\t", "\r", "\n"):
        s = "'" + s
    return s[:32760]


def _sheet(wb, name, headers, rows):
    ws = wb.add_worksheet(name[:31])
    header = wb.add_format({"bold": True, "bg_color": "#12324A", "font_color": "#FFFFFF", "text_wrap": True, "align": "center"})
    text = wb.add_format({"valign": "top"})
    wrap = wb.add_format({"valign": "top", "text_wrap": True})
    money = wb.add_format({"num_format": "#,##0.00", "valign": "top"})
    pct = wb.add_format({"num_format": "0.00%", "valign": "top"})
    for j, h in enumerate(headers):
        ws.write(0, j, h, header)
    for i, row in enumerate(rows, 1):
        for j, h in enumerate(headers):
            v = row.get(h, "")
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                lh = h.lower()
                fmt = pct if any(x in lh for x in ("rate", "ratio", "ctr")) else money if any(x in lh for x in ("spend", "cost", "cpl", "cpa", "cpm", "cpc", "benchmark")) else None
                ws.write_number(i, j, float(v), fmt)
            else:
                sv = _safe(v)
                if sv.startswith(("http://", "https://")):
                    try:
                        ws.write_url(i, j, sv, string=sv)
                    except Exception:
                        ws.write_string(i, j, sv, text)
                else:
                    ws.write_string(i, j, sv, wrap if len(sv) > 60 else text)
    if headers and rows:
        ws.autofilter(0, 0, len(rows), len(headers) - 1)
    ws.freeze_panes(1, 0)
    for j, h in enumerate(headers):
        width = 30 if any(x in h.lower() for x in ("name", "reason", "evidence", "text", "headline", "action", "link", "recommend", "consistency", "observation", "hypothesis")) else 18
        ws.set_column(j, j, min(width, 48))
    if "Classification" in headers and rows:
        c = headers.index("Classification")
        ws.conditional_format(1, c, len(rows), c, {"type": "text", "criteria": "containing", "value": "Winner", "format": wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})})
        ws.conditional_format(1, c, len(rows), c, {"type": "text", "criteria": "containing", "value": "Potential", "format": wb.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500"})})
        ws.conditional_format(1, c, len(rows), c, {"type": "text", "criteria": "containing", "value": "Underperformer", "format": wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})})
    return ws


def _group(rows, keys):
    out = {}
    for r in rows:
        k = tuple(r[x] for x in keys)
        x = out.setdefault(k, {"ads": 0, "spend": 0.0, "results": 0.0, "winners": 0, "potential": 0, "underperformers": 0, "insufficient": 0})
        x["ads"] += 1; x["spend"] += r["spend"]; x["results"] += r["results"]
        x["winners"] += r["classification"] == "Winner"; x["potential"] += r["classification"] == "Potential Winner"
        x["underperformers"] += r["classification"] == "Underperformer"; x["insufficient"] += r["classification"] == "Insufficient Data"
    rows_out = []
    for k, x in out.items():
        y = {keys[i]: k[i] for i in range(len(keys))}; y.update(x)
        y["weighted_cost"] = x["spend"] / x["results"] if x["results"] else None
        y["winner_rate"] = x["winners"] / x["ads"] if x["ads"] else None
        y["potential_rate"] = x["potential"] / x["ads"] if x["ads"] else None
        rows_out.append(y)
    return rows_out



PIPELINE_VERSION = "2.5-checkpointed-ai-directed"
PHASE_BASE = 1
PHASE_DAILY = 2
PHASE_CLASSIFIED = 3
PHASE_SUMMARIES = 4


def _fingerprint(parent_job_id: str, policy: dict) -> str:
    raw = json.dumps(
        {"pipeline": PIPELINE_VERSION, "parent_job_id": parent_job_id, "policy": policy},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:28]


def _analysis_key(parent_job_id: str, fingerprint: str, name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return f"shards/{parent_job_id}/analysis/{PIPELINE_VERSION}/{fingerprint}/{safe}.json.gz"


def _analysis_url(parent_job_id: str, key: str) -> str:
    return f"/internal/jobs/{parent_job_id}/shard?key={urllib.parse.quote(key, safe='')}"


def _put_json_checkpoint(parent_job_id: str, fingerprint: str, name: str, value) -> dict:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    packed = gzip.compress(raw, compresslevel=6)
    if len(packed) > 38_000_000:
        raise RuntimeError(f"Analysis checkpoint {name} is too large ({len(packed)} bytes gzip)")
    key = _analysis_key(parent_job_id, fingerprint, name)
    gateway_raw(_analysis_url(parent_job_id, key), "POST", packed, "application/gzip", timeout=240)
    return {"key": key, "raw_bytes": len(raw), "gzip_bytes": len(packed)}


def _get_json_checkpoint(parent_job_id: str, fingerprint: str, name: str, default=None):
    key = _analysis_key(parent_job_id, fingerprint, name)
    try:
        packed = gateway_raw(_analysis_url(parent_job_id, key), timeout=240)
        return json.loads(gzip.decompress(packed).decode("utf-8"))
    except RuntimeError as ex:
        if "HTTP 404" in str(ex):
            return default
        raise


def _save_state(parent_job_id: str, fingerprint: str, phase: int, artifacts: dict, policy: dict):
    state = {
        "pipeline_version": PIPELINE_VERSION,
        "fingerprint": fingerprint,
        "completed_phase": phase,
        "artifacts": artifacts,
        "policy_sha256": hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    _put_json_checkpoint(parent_job_id, fingerprint, "state", state)
    return state


def _load_state(parent_job_id: str, fingerprint: str):
    state = _get_json_checkpoint(parent_job_id, fingerprint, "state", {}) or {}
    if state.get("pipeline_version") != PIPELINE_VERSION or state.get("fingerprint") != fingerprint:
        return {"completed_phase": 0, "artifacts": {}}
    return state


def _continue_same_job(job_id: str, phase: str, **extra):
    _progress(job_id, phase, checkpoint_saved=True, **extra)
    gateway_json(f"/internal/jobs/{job_id}/continue", "POST", {"reason": f"analysis checkpoint complete: {phase}"})
    return {"phase": phase, "checkpoint_saved": True, **extra}, None, None, True


def _load_metadata(parent_job_id: str, db):
    accounts = {str(r.get("account_id") or _s(r.get("id")).replace("act_", "")): r for r in _iter_rows(parent_job_id, db, "accounts", [])}
    campaigns = {str(r.get("id")): r for r in _iter_rows(parent_job_id, db, "campaigns", [])}
    adsets = {str(r.get("id")): r for r in _iter_rows(parent_job_id, db, "adsets", [])}
    ads = {str(r.get("id")): r for r in _iter_rows(parent_job_id, db, "ads", [])}
    creatives = {}
    for r in _iter_rows(parent_job_id, db, "adcreatives", []):
        cid = str(r.get("id") or "")
        if cid:
            creatives[(str(r.get("account_id") or ""), cid)] = r
            creatives.setdefault(("", cid), r)
    return accounts, campaigns, adsets, ads, creatives


def _build_base_rows(parent_job_id: str, db, p: dict):
    accounts, campaigns, adsets, ads, creatives = _load_metadata(parent_job_id, db)
    lifetime = {}
    for r in _iter_rows(parent_job_id, db, "ad_lifetime", []):
        adid = str(r.get("ad_id") or "")
        if not adid:
            continue
        x = lifetime.setdefault(adid, {"spend":0.0,"impressions":0.0,"reach":0.0,"clicks":0.0,"link_clicks":0.0,"actions":defaultdict(float),"first":"","last":""})
        x["spend"] += _f(r.get("spend")); x["impressions"] += _f(r.get("impressions")); x["reach"] = max(x["reach"], _f(r.get("reach")))
        x["clicks"] += _f(r.get("clicks")); x["link_clicks"] += _f(r.get("inline_link_clicks"))
        for t,v in _actions(r).items(): x["actions"][t] += v
        ds,de=_s(r.get("date_start")),_s(r.get("date_stop"))
        if ds and (not x["first"] or ds<x["first"]): x["first"]=ds
        if de and (not x["last"] or de>x["last"]): x["last"]=de

    rows=[]
    for adid,ad in ads.items():
        account_id=str(ad.get("account_id") or "")
        campaign=campaigns.get(str(ad.get("campaign_id") or ""),{})
        adset=adsets.get(str(ad.get("adset_id") or ""),{})
        cid=str((ad.get("creative") or {}).get("id") or "")
        creative=creatives.get((account_id,cid)) or creatives.get(("",cid)) or {}
        lt=lifetime.get(adid); acts=dict(lt["actions"]) if lt else {}
        family,obj_conf,evidence,name_code=_objective(campaign,adset,creative,acts,p)
        primary=_select_action(family,acts,p)
        results=_f(acts.get(primary)); spend=lt["spend"] if lt else 0.0; impressions=lt["impressions"] if lt else 0.0; reach=lt["reach"] if lt else 0.0; clicks=lt["clicks"] if lt else 0.0; links=lt["link_clicks"] if lt else 0.0
        acc=accounts.get(account_id,{}); business=acc.get("business") or {}; agent_code,agent_name,agent_conf=_agent(campaign.get("name"),p)
        rows.append({
            "account_id":account_id,"account_name":acc.get("name") or "","currency":_s(acc.get("currency")) or "UNKNOWN",
            "business_id":business.get("id","") if isinstance(business,dict) else "","business_name":business.get("name","") if isinstance(business,dict) else "",
            "agent_code":agent_code,"agent_name":agent_name,"agent_confidence":agent_conf,
            "campaign_id":str(ad.get("campaign_id") or ""),"campaign_name":campaign.get("name") or "","campaign_objective":campaign.get("objective") or "","campaign_name_code":name_code,
            "adset_id":str(ad.get("adset_id") or ""),"adset_name":adset.get("name") or "","optimization_goal":adset.get("optimization_goal") or "","destination_type":_s(adset.get("destination_type")).upper() or "UNKNOWN",
            "ad_id":adid,"ad_name":ad.get("name") or "","created_date":ad.get("created_time") or "","creative_id":cid,"creative_type":creative.get("object_type") or "",
            "primary_text":creative.get("body") or "","headline":creative.get("title") or "","cta":creative.get("call_to_action_type") or "","page_id":_nested(creative,["page_id","actor_id"]) or "","form_id":_nested(creative,["lead_gen_form_id","form_id"]) or "",
            "objective_family":family,"objective_confidence":obj_conf,"classification_evidence":evidence,"primary_action":primary,"results":results,"spend":spend,"cost_per_result":spend/results if results else None,
            "impressions":impressions,"reach":reach,"frequency":impressions/reach if reach else None,"cpm":spend*1000/impressions if impressions else None,"clicks":clicks,"link_clicks":links,"cpc":spend/clicks if clicks else None,"ctr":clicks/impressions if impressions else None,"conversion_rate":results/links if links else None,
            "first_delivery_date":lt["first"] if lt else "","last_delivery_date":lt["last"] if lt else "",
            "campaign_link":f"https://www.facebook.com/adsmanager/manage/campaigns?act={account_id}&selected_campaign_ids={str(ad.get('campaign_id') or '')}","ad_link":f"https://www.facebook.com/adsmanager/manage/ads?act={account_id}&selected_ad_ids={adid}","post_link":"Unavailable",
        })
    return rows


def _add_daily_stats(parent_job_id: str, db, rows: list[dict], p: dict):
    by_ad={r["ad_id"]:r for r in rows}; daily=defaultdict(list)
    for d in _iter_rows(parent_job_id,db,"ad_daily",[]):
        adid=str(d.get("ad_id") or ""); r=by_ad.get(adid)
        if not r: continue
        spend=_f(d.get("spend")); results=_f(_actions(d).get(r["primary_action"])) if r["primary_action"] else 0.0
        daily[adid].append((_s(d.get("date_start")),spend,results))
    for r in rows:
        active=[x for x in sorted(daily.get(r["ad_id"],[]),key=lambda x:x[0]) if x[1]>0]
        r["active_days"]=len(active); r["result_days"]=sum(1 for x in active if x[2]>0); r["zero_result_spend_days"]=sum(1 for x in active if x[2]<=0)
        total=sum(x[1] for x in active); zero=sum(x[1] for x in active if x[2]<=0); r["zero_result_spend_ratio"]=zero/total if total else None
        day_costs=[x[1]/x[2] for x in active if x[2]>0]; r["daily_cost_cv"]=_cv(day_costs)
        r["daily_stability"]="Insufficient" if len(day_costs)<2 else "Stable" if r["daily_cost_cv"]<=0.5 else "Moderate" if r["daily_cost_cv"]<=1.0 else "Volatile"
        if active:
            cut=max(1,len(active)//3); early,late=active[:cut],active[-cut:]
            def wc(a):
                s=sum(x[1] for x in a); q=sum(x[2] for x in a); return s/q if q else None
            ec,lc=wc(early),wc(late); ratio=lc/ec if ec and lc else None
            r["early_cost"],r["late_cost"],r["trend_ratio"]=ec,lc,ratio
            r["trend"]="Insufficient" if ratio is None else "Deteriorating" if ratio>=p["trend_deterioration_ratio"] else "Improving" if ratio<=p["trend_improvement_ratio"] else "Stable / Mixed"
        else:
            r["early_cost"]=r["late_cost"]=r["trend_ratio"]=None; r["trend"]="Insufficient"
        r["fatigue"]="Possible fatigue" if r["trend"]=="Deteriorating" and (r.get("frequency") or 0)>=1.5 else "No clear fatigue signal"
    return rows


def _classify_rows(rows: list[dict], p: dict):
    pool_rows=defaultdict(list)
    for r in rows:
        if r["objective_family"]=="Other / Unclassified" or not r["primary_action"]: continue
        for key in [
            ("Account+Destination+Event",(r["currency"],r["objective_family"],r["primary_action"],r["destination_type"],r["account_id"])),
            ("Destination+Event",(r["currency"],r["objective_family"],r["primary_action"],r["destination_type"])),
            ("Objective+Event",(r["currency"],r["objective_family"],r["primary_action"])),
            ("Objective",(r["currency"],r["objective_family"])),
        ]: pool_rows[key].append(r)
    pools={k:_bench(v) for k,v in pool_rows.items()}
    for r in rows:
        scope,b=_pick_benchmark(r,pools,p); r["benchmark_scope"]=scope; r["benchmark_cost"]=b.get("weighted_cost"); r["benchmark_sample"]=b.get("n",0)
        r["performance_vs_benchmark"]=r["cost_per_result"]/r["benchmark_cost"] if r.get("cost_per_result") is not None and r.get("benchmark_cost") else None
        cl,conf,reason,action=_classify(r,b,p); r["classification"]=cl; r["classification_confidence"]=conf; r["classification_reason"]=reason; r["recommended_action"]=action
        r["daily_consistency"]=f"{r['daily_stability']}; {r['trend']}; zero-result days {r['zero_result_spend_days']}/{r['active_days']}"
    return rows


def _build_summary_bundle(parent_job_id: str, db, rows: list[dict], manifest: list[dict]):
    accounts={str(r.get("account_id") or _s(r.get("id")).replace("act_","")):r for r in _iter_rows(parent_job_id,db,"accounts",[])}
    objective_summary=_group([r for r in rows if r["objective_family"]!="Other / Unclassified"],["currency","objective_family","primary_action"])
    agent_summary=_group([r for r in rows if r["agent_name"]!="Unmapped"],["agent_name","currency","objective_family"])
    agent_objective=_group([r for r in rows if r["agent_name"]!="Unmapped" and r["objective_family"]!="Other / Unclassified"],["agent_name","currency","objective_family","primary_action"])
    creative_map={}
    for r in rows:
        cid=r["creative_id"] or "(missing)"; x=creative_map.setdefault(cid,{"Creative ID":cid,"Format":r["creative_type"],"Ads":0,"Spend":0.0,"Results":0.0,"Winners":0,"Potential":0,"Agents":set(),"Primary Text":r["primary_text"],"Headline":r["headline"],"CTA":r["cta"]})
        x["Ads"]+=1;x["Spend"]+=r["spend"];x["Results"]+=r["results"];x["Winners"]+=r["classification"]=="Winner";x["Potential"]+=r["classification"]=="Potential Winner";x["Agents"].add(r["agent_name"])
    creative_rows=[]
    for x in creative_map.values():
        y=dict(x);y["Weighted Cost"]=x["Spend"]/x["Results"] if x["Results"] else None;y["Agents"]=", ".join(sorted(x["Agents"]));y["Observation"]="Repeated Winner evidence" if x["Winners"]>=2 else "Repeated Potential evidence" if x["Potential"]>=2 else "No repeated positive classification";y["Hypothesis"]="Candidate for controlled reuse in comparable objective/event groups" if x["Winners"]>=2 else "Requires further validation";creative_rows.append(y)
    creative_rows.sort(key=lambda x:(x["Winners"],x["Results"],x["Spend"]),reverse=True)
    dataset_status={x["name"]:x.get("status") for x in manifest}; required={"accounts","campaigns","adsets","ads","adcreatives","ad_lifetime","ad_daily"};audit_status="COMPLETE" if all(dataset_status.get(x)=="done" for x in required) else "PARTIAL"
    errors=db.execute("SELECT dataset,scope_id,error FROM errors ORDER BY dataset,scope_id").fetchall(); ad_count=Counter(r["account_id"] for r in rows)
    dates_by_account=defaultdict(list)
    for r in rows:
        if r.get("first_delivery_date"):dates_by_account[r["account_id"]].append(r["first_delivery_date"])
        if r.get("last_delivery_date"):dates_by_account[r["account_id"]].append(r["last_delivery_date"])
    coverage=[]
    for aid,a in accounts.items():
        dates=dates_by_account.get(aid,[]); errs=[e for e in errors if _s(e["scope_id"]).startswith(aid)]
        business=a.get("business") or {}
        coverage.append({"Account Name":a.get("name") or "","Account ID":aid,"Business ID":business.get("id","") if isinstance(business,dict) else "","Account Status":a.get("account_status"),"Currency":a.get("currency") or "","Earliest Date":min(dates) if dates else "","Latest Date":max(dates) if dates else "","Scan Status":"PARTIAL" if errs else "Core performance scanned","Ads Retrieved":ad_count.get(aid,0),"API Errors":" | ".join(_s(e["error"])[:180] for e in errs[:3]),"Missing Ranges":"See error scopes" if errs else "None detected in completed scopes","Data Completeness":"Post permalink unavailable"+("; other errors present" if errs else "")})
    all_dates=[d for v in dates_by_account.values() for d in v]
    return {"objective_summary":objective_summary,"agent_summary":agent_summary,"agent_objective":agent_objective,"creative_rows":creative_rows,"coverage_rows":coverage,"audit_status":audit_status,"earliest":min(all_dates) if all_dates else "","latest":max(all_dates) if all_dates else "","total_accounts":len(accounts)}


def _write_workbook(rows: list[dict], bundle: dict, p: dict, parent_job_id: str) -> bytes:
    fd,out_path=tempfile.mkstemp(prefix="elokaby_final_",suffix=".xlsx");os.close(fd)
    try:
        wb=xlsxwriter.Workbook(out_path,{"constant_memory":True});title=wb.add_format({"bold":True,"font_size":16,"font_color":"#FFFFFF","bg_color":"#12324A"});label=wb.add_format({"bold":True,"bg_color":"#EAF0F4"})
        ws=wb.add_worksheet("Executive Summary");ws.merge_range("A1:H1",p["report_title"],title);cls=Counter(r["classification"] for r in rows);obj=Counter(r["objective_family"] for r in rows)
        summary=[("Audit Status",bundle["audit_status"]),("Parent Job ID",parent_job_id),("Total Ad Accounts",bundle["total_accounts"]),("Earliest Date",bundle["earliest"]),("Latest Date",bundle["latest"]),("Total Ads",len(rows)),("Lead Generation Ads",obj["Lead Generation"]),("Conversion Ads",obj["Conversion"]),("Lead Messaging Ads",obj["Lead Messaging"]),("WhatsApp Ads",obj["WhatsApp"]),("Other / Unclassified",obj["Other / Unclassified"]),("Winners",cls["Winner"]),("Potential Winners",cls["Potential Winner"]),("Underperformers",cls["Underperformer"]),("Insufficient Data",cls["Insufficient Data"]),("Analysis Engine","AI-directed policy + deterministic calculations + checkpointed R2 execution"),("Coverage limitation","Post permalinks unavailable where post_objects is unavailable; Ads Manager links included.")]
        for i,(k,v) in enumerate(summary,2):ws.write(i-1,0,k,label);ws.write(i-1,1,v)
        headers=["Currency","Objective Family","Primary Result Event","Ads","Spend","Results","Weighted Cost","Winners","Potential","Underperformers","Insufficient"]
        for j,h in enumerate(headers):ws.write(1,3+j,h,label)
        for i,r in enumerate(sorted(bundle["objective_summary"],key=lambda x:(x["currency"],x["objective_family"],x["primary_action"])),2):
            for j,v in enumerate([r["currency"],r["objective_family"],r["primary_action"],r["ads"],r["spend"],r["results"],r["weighted_cost"],r["winners"],r["potential"],r["underperformers"],r["insufficient"]]):ws.write(i,3+j,v)
        ws.set_column("A:A",28);ws.set_column("B:B",55);ws.set_column("D:N",18);ws.freeze_panes(1,0)
        _sheet(wb,"Scan Coverage & Errors",list(bundle["coverage_rows"][0].keys()) if bundle["coverage_rows"] else [],bundle["coverage_rows"])
        headers_all=["Agent Code","Agent Name","Mapping Confidence","Business","Ad Account","Account ID","Currency","Campaign","Campaign ID","Campaign Link","Campaign Name Code","Detected Objective Family","Objective Confidence","Classification Evidence","Ad Set","Ad Set ID","Optimization Goal","Destination Type","Ad","Ad ID","Creative ID","Creative Type","Created Date","First Delivery Date","Last Delivery Date","Active Days","Result Days","Zero-result spend days","Spend","Primary Results","Primary Action Type","Cost Per Result","Impressions","Reach","Frequency","CPM","Clicks","Link Clicks","CPC","CTR","Conversion Rate","Primary Text","Headline","CTA","Page","Form","Classification","Classification Confidence","Benchmark","Benchmark Scope","Benchmark Sample Size","Performance vs Benchmark","Daily Consistency","Classification Reason","Recommended Action","Ad Link","Original Post Link"]
        def flat(r):
            return {"Agent Code":r["agent_code"],"Agent Name":r["agent_name"],"Mapping Confidence":r["agent_confidence"],"Business":r["business_name"],"Ad Account":r["account_name"],"Account ID":r["account_id"],"Currency":r["currency"],"Campaign":r["campaign_name"],"Campaign ID":r["campaign_id"],"Campaign Link":r["campaign_link"],"Campaign Name Code":r["campaign_name_code"],"Detected Objective Family":r["objective_family"],"Objective Confidence":r["objective_confidence"],"Classification Evidence":r["classification_evidence"],"Ad Set":r["adset_name"],"Ad Set ID":r["adset_id"],"Optimization Goal":r["optimization_goal"],"Destination Type":r["destination_type"],"Ad":r["ad_name"],"Ad ID":r["ad_id"],"Creative ID":r["creative_id"],"Creative Type":r["creative_type"],"Created Date":r["created_date"],"First Delivery Date":r["first_delivery_date"],"Last Delivery Date":r["last_delivery_date"],"Active Days":r["active_days"],"Result Days":r["result_days"],"Zero-result spend days":r["zero_result_spend_days"],"Spend":r["spend"],"Primary Results":r["results"],"Primary Action Type":r["primary_action"],"Cost Per Result":r["cost_per_result"],"Impressions":r["impressions"],"Reach":r["reach"],"Frequency":r["frequency"],"CPM":r["cpm"],"Clicks":r["clicks"],"Link Clicks":r["link_clicks"],"CPC":r["cpc"],"CTR":r["ctr"],"Conversion Rate":r["conversion_rate"],"Primary Text":r["primary_text"],"Headline":r["headline"],"CTA":r["cta"],"Page":r["page_id"],"Form":r["form_id"],"Classification":r["classification"],"Classification Confidence":r["classification_confidence"],"Benchmark":r["benchmark_cost"],"Benchmark Scope":r["benchmark_scope"],"Benchmark Sample Size":r["benchmark_sample"],"Performance vs Benchmark":r["performance_vs_benchmark"],"Daily Consistency":r["daily_consistency"],"Classification Reason":r["classification_reason"],"Recommended Action":r["recommended_action"],"Ad Link":r["ad_link"],"Original Post Link":r["post_link"]}
        _sheet(wb,"All Classified Ads",headers_all,[flat(r) for r in rows]);_sheet(wb,"Lead Gen Winners",headers_all,[flat(r) for r in rows if r["objective_family"]=="Lead Generation" and r["classification"]=="Winner"]);_sheet(wb,"Conversion Winners",headers_all,[flat(r) for r in rows if r["objective_family"]=="Conversion" and r["classification"]=="Winner"]);_sheet(wb,"Lead Messaging Winners",headers_all,[flat(r) for r in rows if r["objective_family"]=="Lead Messaging" and r["classification"]=="Winner"]);_sheet(wb,"WhatsApp Winners",headers_all,[flat(r) for r in rows if r["objective_family"]=="WhatsApp" and r["classification"]=="Winner"])
        ph=["Objective Family","Agent","Currency","Ad Account","Campaign","Campaign Link","Ad","Ad ID","Spend","Results","Primary Action","Cost Per Result","Benchmark","Why Potential","Missing Evidence","Testing Recommendation","Confidence","Ad Link"];pr=[]
        for r in rows:
            if r["classification"]!="Potential Winner":continue
            miss=[]
            if r["results"]<p["winner_min_results"]:miss.append("volume")
            if r["active_days"]<p["winner_min_active_days"]:miss.append("delivery days")
            if r["result_days"]<p["winner_min_result_days"]:miss.append("result-day consistency")
            pr.append({"Objective Family":r["objective_family"],"Agent":r["agent_name"],"Currency":r["currency"],"Ad Account":r["account_name"],"Campaign":r["campaign_name"],"Campaign Link":r["campaign_link"],"Ad":r["ad_name"],"Ad ID":r["ad_id"],"Spend":r["spend"],"Results":r["results"],"Primary Action":r["primary_action"],"Cost Per Result":r["cost_per_result"],"Benchmark":r["benchmark_cost"],"Why Potential":r["classification_reason"],"Missing Evidence":", ".join(miss) or "scale proof","Testing Recommendation":r["recommended_action"],"Confidence":r["classification_confidence"],"Ad Link":r["ad_link"]})
        _sheet(wb,"Potential Winners",ph,pr);_sheet(wb,"Agent Performance",list(bundle["agent_summary"][0].keys()) if bundle["agent_summary"] else [],bundle["agent_summary"]);_sheet(wb,"Agent × Objective",list(bundle["agent_objective"][0].keys()) if bundle["agent_objective"] else [],bundle["agent_objective"]);_sheet(wb,"Agent Winners",headers_all,[flat(r) for r in rows if r["classification"]=="Winner" and r["agent_name"]!="Unmapped"])
        ch=["Creative ID","Format","Ads","Spend","Results","Weighted Cost","Winners","Potential","Agents","Primary Text","Headline","CTA","Observation","Hypothesis"];_sheet(wb,"Creative Insights",ch,bundle["creative_rows"])
        dh=["Agent","Objective Family","Currency","Ad Account","Campaign","Campaign Link","Ad","Ad ID","Classification","Active Days","Result Days","Zero-result spend days","Lifetime Spend","Lifetime Results","Weighted Cost","Daily Cost CV","Daily Stability","Early Cost","Late Cost","Trend","Fatigue Assessment","Ad Link"];dr=[]
        for r in rows:
            if r["classification"] not in ("Winner","Potential Winner"):continue
            dr.append({"Agent":r["agent_name"],"Objective Family":r["objective_family"],"Currency":r["currency"],"Ad Account":r["account_name"],"Campaign":r["campaign_name"],"Campaign Link":r["campaign_link"],"Ad":r["ad_name"],"Ad ID":r["ad_id"],"Classification":r["classification"],"Active Days":r["active_days"],"Result Days":r["result_days"],"Zero-result spend days":r["zero_result_spend_days"],"Lifetime Spend":r["spend"],"Lifetime Results":r["results"],"Weighted Cost":r["cost_per_result"],"Daily Cost CV":r["daily_cost_cv"],"Daily Stability":r["daily_stability"],"Early Cost":r["early_cost"],"Late Cost":r["late_cost"],"Trend":r["trend"],"Fatigue Assessment":r["fatigue"],"Ad Link":r["ad_link"]})
        _sheet(wb,"Daily Consistency",dh,dr);wb.close();return Path(out_path).read_bytes()
    finally:
        try:os.remove(out_path)
        except OSError:pass


def run_stored_analysis_job(job: dict):
    params=(job.get("input") or {}).get("params") or {}; parent_job_id=str(params.get("parent_job_id") or "")
    if not parent_job_id: raise ValueError("parent_job_id is required")
    supplied=params.get("analysis_policy") or {}
    if not supplied:
        raise ValueError("AI analysis_policy is required for __FULL_AUDIT__; fixed backend defaults must not be the sole decision source")
    p=_defaults(supplied);job_id=job["job_id"];fingerprint=_fingerprint(parent_job_id,p);state=_load_state(parent_job_id,fingerprint);phase=int(state.get("completed_phase") or 0);artifacts=dict(state.get("artifacts") or {})
    _progress(job_id,"resume_check",parent_job_id=parent_job_id,analysis_fingerprint=fingerprint,resuming_from_phase=phase,pipeline_version=PIPELINE_VERSION)
    path=_load_parent(parent_job_id);db=None
    try:
        db=sqlite3.connect(path);db.row_factory=sqlite3.Row;manifest=_manifest(db);available={x["name"] for x in manifest};required={"accounts","campaigns","adsets","ads","adcreatives","ad_lifetime","ad_daily"};missing=sorted(required-available)
        if missing:raise ValueError("Missing required parent datasets: "+", ".join(missing))
        if phase<PHASE_BASE:
            _progress(job_id,"phase_1_base_rows",checkpoint="pending")
            rows=_build_base_rows(parent_job_id,db,p);artifacts["base_rows"]=_put_json_checkpoint(parent_job_id,fingerprint,"base_rows",rows);state=_save_state(parent_job_id,fingerprint,PHASE_BASE,artifacts,p)
            return _continue_same_job(job_id,"phase_1_complete",rows=len(rows),analysis_fingerprint=fingerprint)
        if phase<PHASE_DAILY:
            _progress(job_id,"phase_2_daily_consistency",checkpoint="pending")
            rows=_get_json_checkpoint(parent_job_id,fingerprint,"base_rows")
            if rows is None:raise RuntimeError("base_rows checkpoint missing")
            rows=_add_daily_stats(parent_job_id,db,rows,p);artifacts["daily_rows"]=_put_json_checkpoint(parent_job_id,fingerprint,"daily_rows",rows);state=_save_state(parent_job_id,fingerprint,PHASE_DAILY,artifacts,p)
            return _continue_same_job(job_id,"phase_2_complete",rows=len(rows),analysis_fingerprint=fingerprint)
        if phase<PHASE_CLASSIFIED:
            _progress(job_id,"phase_3_benchmarks_classification",checkpoint="pending")
            rows=_get_json_checkpoint(parent_job_id,fingerprint,"daily_rows")
            if rows is None:raise RuntimeError("daily_rows checkpoint missing")
            rows=_classify_rows(rows,p);artifacts["classified_rows"]=_put_json_checkpoint(parent_job_id,fingerprint,"classified_rows",rows);state=_save_state(parent_job_id,fingerprint,PHASE_CLASSIFIED,artifacts,p)
            return _continue_same_job(job_id,"phase_3_complete",rows=len(rows),analysis_fingerprint=fingerprint)
        if phase<PHASE_SUMMARIES:
            _progress(job_id,"phase_4_summaries",checkpoint="pending")
            rows=_get_json_checkpoint(parent_job_id,fingerprint,"classified_rows")
            if rows is None:raise RuntimeError("classified_rows checkpoint missing")
            bundle=_build_summary_bundle(parent_job_id,db,rows,manifest);artifacts["summary_bundle"]=_put_json_checkpoint(parent_job_id,fingerprint,"summary_bundle",bundle);state=_save_state(parent_job_id,fingerprint,PHASE_SUMMARIES,artifacts,p)
            return _continue_same_job(job_id,"phase_4_complete",rows=len(rows),analysis_fingerprint=fingerprint,audit_status=bundle["audit_status"])
        _progress(job_id,"phase_5_workbook",checkpoint="loaded")
        rows=_get_json_checkpoint(parent_job_id,fingerprint,"classified_rows");bundle=_get_json_checkpoint(parent_job_id,fingerprint,"summary_bundle")
        if rows is None or bundle is None:raise RuntimeError("Final analysis checkpoints missing")
        payload=_write_workbook(rows,bundle,p,parent_job_id);cls=Counter(r["classification"] for r in rows);obj=Counter(r["objective_family"] for r in rows)
        result={"parent_job_id":parent_job_id,"analysis_fingerprint":fingerprint,"pipeline_version":PIPELINE_VERSION,"audit_status":bundle["audit_status"],"checkpointed":True,"completed_phase":5,"total_accounts":bundle["total_accounts"],"total_ads":len(rows),"earliest_date":bundle["earliest"],"latest_date":bundle["latest"],"objective_counts":dict(obj),"classification_counts":dict(cls),"winner_counts_by_objective":dict(Counter(r["objective_family"] for r in rows if r["classification"]=="Winner")),"potential_counts_by_objective":dict(Counter(r["objective_family"] for r in rows if r["classification"]=="Potential Winner")),"methodology":"Hybrid AI-directed audit: ChatGPT supplies the analysis policy; deterministic code executes raw joins, weighted math, daily statistics, policy rules, checkpoints and XLSX generation. No Meta re-extraction.","resume_behavior":"Normal execution uses one logical child job_id across checkpoint continuations. If a hard infrastructure failure marks that child failed, launching the same full-audit request again with the same policy automatically reuses the parent-R2 checkpoint fingerprint and resumes from the last completed phase.","coverage_note":"Parent manifest/errors determine COMPLETE vs PARTIAL; unavailable post permalinks are never fabricated."}
        _progress(job_id,"done",total_ads=len(rows),winners=cls.get("Winner",0),analysis_fingerprint=fingerprint);return result,payload,XLSX_MIME,False
    finally:
        try:
            if db is not None:db.close()
        except Exception:pass
        try:os.remove(path)
        except OSError:pass
