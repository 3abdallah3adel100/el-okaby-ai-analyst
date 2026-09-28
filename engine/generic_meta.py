"""Generic read-only Meta Graph/Marketing API execution for AI-directed plans.

The AI chooses WHAT data is needed. This module only validates a declarative read plan,
executes safe GET requests, paginates, and returns raw Meta data. It contains no Winner,
objective, agent, or report-specific business logic.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Iterable

from .meta import MetaClient, MetaError, clean_account

FIELD_EXPR = re.compile(r"^[A-Za-z0-9_.,{}():-]{1,3000}$")
NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,90}$")
OBJECT_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,160}$")
LEVELS = {"account", "campaign", "adset", "ad"}
ACCOUNT_COLLECTIONS = {"campaigns", "adsets", "ads", "adcreatives"}
SOURCES = {"accounts", "campaigns", "adsets", "ads", "adcreatives", "insights", "objects", "edge"}

CAPABILITIES = {
    "design": "AI-directed read-only Meta data engine. The caller chooses resources, fields, dates, levels and breakdowns; Meta remains the final field-compatibility validator.",
    "sources": sorted(SOURCES),
    "account_collections": sorted(ACCOUNT_COLLECTIONS),
    "insights_levels": sorted(LEVELS),
    "supports": [
        "automatic all-token account discovery",
        "arbitrary safe Meta field expressions including nested Graph fields",
        "campaign/adset/ad/adcreative collection reads",
        "Meta Insights with arbitrary requested metrics/breakdowns",
        "generic object reads for discovered Graph object IDs (posts, creatives, forms, pages when token permissions allow)",
        "generic read-only object edges",
        "pagination",
        "custom/max/date-preset time ranges",
        "raw actions/action_values/cost_per_action_type",
        "long-running checkpointed analysis jobs",
    ],
    "important": [
        "No write endpoints are exposed.",
        "A requested field is not guaranteed to be compatible with every resource/breakdown; Meta validates it at execution.",
        "Messaging, leads, purchases and other action types are returned raw unless the caller explicitly asks for a deterministic calculation.",
    ],
}


def _field_expr_list(value: Any, *, max_items: int = 120) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not value or len(value) > max_items:
        raise ValueError("fields must be a non-empty list")
    out = []
    for item in value:
        s = str(item).strip()
        if not FIELD_EXPR.fullmatch(s):
            raise ValueError(f"Unsafe/invalid Meta field expression: {s[:80]}")
        if s not in out:
            out.append(s)
    return out


def _names(value: Any, *, max_items: int = 12) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > max_items:
        raise ValueError("Invalid name list")
    out = []
    for item in value:
        s = str(item).strip()
        if not NAME.fullmatch(s):
            raise ValueError("Invalid Meta name/breakdown")
        if s not in out:
            out.append(s)
    return out


def _date(v: Any) -> str:
    try:
        return dt.date.fromisoformat(str(v)).isoformat()
    except ValueError as ex:
        raise ValueError("Dates must use YYYY-MM-DD") from ex


def _time_range(value: Any) -> dict:
    if value is None:
        return {"mode": "preset", "date_preset": "today"}
    if not isinstance(value, dict):
        raise ValueError("time_range must be an object")
    mode = str(value.get("mode") or "preset")
    if mode == "maximum":
        return {"mode": "maximum", "date_preset": "maximum"}
    if mode == "custom":
        since, until = _date(value.get("since")), _date(value.get("until"))
        if since > until:
            raise ValueError("since cannot be after until")
        return {"mode": "custom", "since": since, "until": until}
    if mode == "preset":
        preset = str(value.get("date_preset") or "today")
        if not re.fullmatch(r"[a-z0-9_]{1,60}", preset):
            raise ValueError("Invalid date_preset")
        return {"mode": "preset", "date_preset": preset}
    raise ValueError("Unsupported time_range.mode")


def validate_read_spec(spec: dict, *, inherited_time_range: dict | None = None, quick: bool = False) -> dict:
    if not isinstance(spec, dict):
        raise ValueError("read spec must be an object")
    source = str(spec.get("source") or "").strip().lower()
    if source not in SOURCES:
        raise ValueError(f"Unsupported source: {source}")
    name = str(spec.get("name") or source).strip()
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,80}", name):
        raise ValueError("Invalid dataset name")
    fields = [] if source == "accounts" and not spec.get("fields") else _field_expr_list(spec.get("fields") or ["id", "name"])
    account_ids = spec.get("account_ids") or []
    if not isinstance(account_ids, list) or len(account_ids) > 200:
        raise ValueError("Invalid account_ids")
    account_ids = list(dict.fromkeys(clean_account(x) for x in account_ids))
    row_limit = int(spec.get("row_limit") or (3000 if quick else 5_000_000))
    if row_limit < 1 or row_limit > (10_000 if quick else 10_000_000):
        raise ValueError("row_limit outside allowed range")
    page_limit = int(spec.get("page_limit") or (50 if quick else 100_000))
    if page_limit < 1 or page_limit > (100 if quick else 250_000):
        raise ValueError("page_limit outside allowed range")
    tr = _time_range(spec.get("time_range") if "time_range" in spec else inherited_time_range)
    out = {
        "name": name,
        "source": source,
        "fields": fields,
        "account_ids": account_ids,
        "row_limit": row_limit,
        "page_limit": page_limit,
        "time_range": tr,
        "breakdowns": _names(spec.get("breakdowns"), max_items=8),
        "level": str(spec.get("level") or "ad"),
        "time_increment": spec.get("time_increment"),
        "filtering": spec.get("filtering") or [],
        "limit": min(500, max(1, int(spec.get("limit") or 200))),
    }
    if source == "insights" and out["level"] not in LEVELS:
        raise ValueError("Invalid insights level")
    if not isinstance(out["filtering"], list) or len(json.dumps(out["filtering"])) > 12000:
        raise ValueError("Invalid filtering")
    if out["time_increment"] is not None:
        ti = str(out["time_increment"])
        if not re.fullmatch(r"(?:1|7|monthly|all_days|\d{1,3})", ti):
            raise ValueError("Invalid time_increment")
        out["time_increment"] = ti
    if source in ("objects", "edge"):
        ids = spec.get("object_ids") or []
        ref = spec.get("object_ids_from")
        max_ids = 200 if quick else 50_000
        if ids:
            if not isinstance(ids, list) or len(ids) > max_ids:
                raise ValueError("Invalid object_ids")
            out["object_ids"] = [str(x) for x in ids]
            if any(not OBJECT_ID.fullmatch(x) for x in out["object_ids"]):
                raise ValueError("Invalid object id")
        elif ref:
            if quick:
                raise ValueError("object_ids_from is only supported inside start_analysis_job")
            if not isinstance(ref, dict):
                raise ValueError("object_ids_from must be an object")
            ds = str(ref.get("dataset") or "")
            field = str(ref.get("field") or "")
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,80}", ds) or not re.fullmatch(r"[A-Za-z0-9_.]{1,160}", field):
                raise ValueError("Invalid object_ids_from")
            out["object_ids_from"] = {"dataset": ds, "field": field}
            out["object_ids"] = []
        else:
            raise ValueError(f"{source} source requires object_ids or object_ids_from")
        if source == "edge":
            edge = str(spec.get("edge") or "")
            if not NAME.fullmatch(edge):
                raise ValueError("Invalid edge")
            out["edge"] = edge
    return out


def validate_plan(plan: dict) -> dict:
    if not isinstance(plan, dict):
        raise ValueError("analysis plan must be an object")
    original = str(plan.get("original_request") or "")[:12000]
    scope = plan.get("account_scope") or {"mode": "all_accessible"}
    if not isinstance(scope, dict):
        raise ValueError("account_scope must be an object")
    mode = str(scope.get("mode") or "all_accessible")
    if mode not in {"all_accessible", "selected"}:
        raise ValueError("Invalid account scope")
    selected = scope.get("account_ids") or []
    if mode == "selected":
        if not isinstance(selected, list) or not selected or len(selected) > 200:
            raise ValueError("selected account scope requires account_ids")
        selected = list(dict.fromkeys(clean_account(x) for x in selected))
    else:
        selected = []
    tr = _time_range(plan.get("time_range") or {"mode": "preset", "date_preset": "today"})
    datasets = plan.get("datasets")
    if not isinstance(datasets, list) or not datasets or len(datasets) > 40:
        raise ValueError("datasets must contain 1..40 read specs")
    validated = []
    names = set()
    for raw in datasets:
        x = validate_read_spec(raw, inherited_time_range=tr, quick=False)
        if x["name"] in names:
            raise ValueError("Dataset names must be unique")
        names.add(x["name"])
        if selected and not x["account_ids"] and x["source"] in (ACCOUNT_COLLECTIONS | {"insights"}):
            x["account_ids"] = selected
        validated.append(x)
    output = plan.get("output") or {}
    if not isinstance(output, dict):
        raise ValueError("output must be an object")
    return {
        "original_request": original,
        "account_scope": {"mode": mode, "account_ids": selected},
        "time_range": tr,
        "datasets": validated,
        "output": {
            "preview_rows_per_dataset": min(25, max(0, int(output.get("preview_rows_per_dataset") or 5))),
            "persist_dataset": True,
        },
    }


def _account_index(client: MetaClient) -> tuple[dict[str, dict], dict]:
    discovery = client.discover()
    return {str(a["id"]): a for a in discovery.get("accounts", [])}, discovery


def resolve_accounts(client: MetaClient, requested: list[str] | None) -> tuple[list[dict], dict]:
    index, discovery = _account_index(client)
    if requested:
        missing = [a for a in requested if a not in index]
        if missing:
            raise ValueError("Some requested account IDs are not accessible: " + ",".join(missing[:10]))
        rows = [index[a] for a in requested]
    else:
        rows = list(index.values())
    return rows, discovery


def _query_params(spec: dict) -> dict:
    q = {"fields": ",".join(spec["fields"]), "limit": spec["limit"]}
    if spec["source"] == "insights":
        q.update({
            "level": spec["level"],
            "breakdowns": ",".join(spec["breakdowns"]) if spec["breakdowns"] else None,
            "filtering": json.dumps(spec["filtering"], separators=(",", ":")) if spec["filtering"] else None,
            "time_increment": spec["time_increment"],
        })
        tr = spec["time_range"]
        if tr["mode"] == "custom":
            q["time_range"] = json.dumps({"since": tr["since"], "until": tr["until"]}, separators=(",", ":"))
        else:
            q["date_preset"] = tr["date_preset"]
    return {k: v for k, v in q.items() if v is not None}


def pages_for_account(client: MetaClient, spec: dict, account: dict, *, after: str | None = None) -> Iterable[tuple[list[dict], str | None]]:
    aid = str(account["id"])
    alias = str(account["token_alias"])
    if spec["source"] == "insights":
        path = f"/act_{aid}/insights"
    elif spec["source"] in ACCOUNT_COLLECTIONS:
        path = f"/act_{aid}/{spec['source']}"
    else:
        raise ValueError("pages_for_account called for non-account source")
    q = _query_params(spec)
    cursor = after
    seen = set()
    for _ in range(spec["page_limit"]):
        payload, _usage = client.request(path, alias, {**q, **({"after": cursor} if cursor else {})})
        data = payload.get("data", []) if isinstance(payload, dict) else []
        rows = []
        for item in data:
            if isinstance(item, dict):
                x = dict(item)
                x.setdefault("account_id", aid)
                rows.append(x)
        paging = payload.get("paging", {}) if isinstance(payload, dict) else {}
        nxt = (paging.get("cursors") or {}).get("after") if paging.get("next") else None
        if nxt == cursor or nxt in seen:
            nxt = None
        if nxt:
            seen.add(nxt)
        yield rows, str(nxt) if nxt else None
        if not nxt:
            break
        cursor = str(nxt)



def account_rows(client: MetaClient, spec: dict, accounts: list[dict]) -> list[dict]:
    """Read requested account fields; discovery remains the source of authorization scope."""
    if not spec.get("fields"):
        return [{k: v for k, v in a.items() if k not in ("token_alias", "token_aliases")} for a in accounts]
    out=[]
    for a in accounts:
        aid=str(a["id"]);alias=str(a["token_alias"])
        payload,_=client.request("/act_"+aid,alias,{"fields":",".join(spec["fields"])})
        if isinstance(payload,dict):
            row=dict(payload);row.setdefault("account_id",aid);out.append(row)
    return out

def object_rows(client: MetaClient, spec: dict, account_hint: dict | None = None) -> list[dict]:
    alias = str(account_hint["token_alias"]) if account_hint else next(iter(client.tokens))
    out = []
    for oid in spec.get("object_ids", []):
        path = f"/{oid}" + (f"/{spec['edge']}" if spec["source"] == "edge" else "")
        q = {"fields": ",".join(spec["fields"]), "limit": spec["limit"]}
        cursor = None
        for _ in range(spec["page_limit"] if spec["source"] == "edge" else 1):
            payload, _ = client.request(path, alias, {**q, **({"after": cursor} if cursor else {})})
            if spec["source"] == "edge":
                data = payload.get("data", []) if isinstance(payload, dict) else []
                for item in data:
                    if isinstance(item, dict):
                        x = dict(item); x.setdefault("__object_id", oid); out.append(x)
                paging = payload.get("paging", {}) if isinstance(payload, dict) else {}
                nxt = (paging.get("cursors") or {}).get("after") if paging.get("next") else None
                if not nxt or nxt == cursor:
                    break
                cursor = str(nxt)
            else:
                if isinstance(payload, dict):
                    x = dict(payload); x.setdefault("__object_id", oid); out.append(x)
                break
            if len(out) >= spec["row_limit"]:
                return out[:spec["row_limit"]]
    return out[:spec["row_limit"]]


def quick_read(client: MetaClient, raw_spec: dict) -> dict:
    spec = validate_read_spec(raw_spec, quick=True)
    if spec["source"] == "accounts":
        accounts, discovery = resolve_accounts(client, spec["account_ids"])
        rows = account_rows(client, spec, accounts)
        return {"source": "accounts", "rows": rows[:spec["row_limit"]], "row_count": len(rows), "discovery_complete": discovery.get("discovery_complete", False), "errors": discovery.get("errors", [])}
    accounts, discovery = resolve_accounts(client, spec["account_ids"])
    if spec["source"] in ACCOUNT_COLLECTIONS or spec["source"] == "insights":
        rows, errors = [], []
        for account in accounts:
            try:
                for page, _nxt in pages_for_account(client, spec, account):
                    remaining = spec["row_limit"] - len(rows)
                    rows.extend(page[:remaining])
                    if len(rows) >= spec["row_limit"]:
                        return {"source": spec["source"], "rows": rows, "row_count": len(rows), "truncated": True, "errors": errors, "discovery_complete": discovery.get("discovery_complete", False)}
            except Exception as ex:
                errors.append({"account_id": account["id"], "error": str(ex)[:300]})
        return {"source": spec["source"], "rows": rows, "row_count": len(rows), "truncated": False, "errors": errors, "discovery_complete": discovery.get("discovery_complete", False)}
    hint = accounts[0] if accounts else None
    rows = object_rows(client, spec, hint)
    return {"source": spec["source"], "rows": rows, "row_count": len(rows), "truncated": len(rows) >= spec["row_limit"], "errors": []}
