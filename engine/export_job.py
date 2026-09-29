"""Private raw exports from an already persisted start_analysis_job parent dataset.

No Meta API calls are made here. Data is streamed from the parent's SQLite metadata and
R2 shards, then written to bounded downloadable artifacts.
"""
from __future__ import annotations

import csv
import io
import json
import os
import sqlite3
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable

import xlsxwriter

from .dataset_tools import _iter_rows, _load_parent, _manifest

ZIP_MIME = "application/zip"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _json_scalar(v):
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    return str(v)


def _safe_excel(v):
    s = _json_scalar(v)
    if s[:1] in ("=", "+", "-", "@", "\t", "\r", "\n"):
        return "'" + s
    return s[:32760]


def _dataset_names(db: sqlite3.Connection, requested: list[str] | None) -> list[str]:
    available = [str(r[0]) for r in db.execute("SELECT name FROM datasets ORDER BY rowid")]
    if not requested:
        return [x for x in available if x != "post_objects" or _dataset_count(db, x) > 0]
    seen = []
    for x in requested:
        name = str(x)
        if name not in available:
            raise ValueError(f"Unknown dataset: {name}")
        if name not in seen:
            seen.append(name)
    return seen


def _dataset_count(db: sqlite3.Connection, dataset: str) -> int:
    r = db.execute("SELECT row_count FROM datasets WHERE name=?", (dataset,)).fetchone()
    return int(r[0] or 0) if r else 0


def _columns(parent_job_id: str, db: sqlite3.Connection, dataset: str, *, offset=0, limit=None) -> list[str]:
    keys = []
    seen = set()
    i = 0
    taken = 0
    for row in _iter_rows(parent_job_id, db, dataset, []):
        if i < offset:
            i += 1
            continue
        if limit is not None and taken >= limit:
            break
        i += 1
        taken += 1
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                keys.append(str(k))
    priority = [
        "account_id","account_name","date_start","date_stop","campaign_id","campaign_name",
        "adset_id","adset_name","ad_id","ad_name","spend","impressions","reach","frequency",
        "cpm","clicks","inline_link_clicks","cpc","ctr","actions","action_values","cost_per_action_type"
    ]
    return [k for k in priority if k in seen] + [k for k in keys if k not in priority]


def _iter_slice(parent_job_id: str, db: sqlite3.Connection, dataset: str, offset: int, limit: int | None):
    idx = 0
    yielded = 0
    for row in _iter_rows(parent_job_id, db, dataset, []):
        if idx < offset:
            idx += 1
            continue
        if limit is not None and yielded >= limit:
            break
        idx += 1
        yielded += 1
        yield row


def _write_xlsx(parent_job_id: str, db: sqlite3.Connection, datasets: list[str], slices: dict[str, tuple[int,int|None]]) -> bytes:
    fd, path = tempfile.mkstemp(prefix="elokaby_raw_", suffix=".xlsx")
    os.close(fd)
    try:
        wb = xlsxwriter.Workbook(path, {"constant_memory": True})
        header = wb.add_format({"bold": True, "bg_color": "#12324A", "font_color": "#FFFFFF", "text_wrap": True})
        text_fmt = wb.add_format({"num_format": "@"})
        used_names = set()
        for ds in datasets:
            off, lim = slices.get(ds, (0, None))
            cols = _columns(parent_job_id, db, ds, offset=off, limit=lim)
            base = ds[:31] or "Data"
            sheet = base
            n = 2
            while sheet in used_names:
                suffix = f"_{n}"
                sheet = (base[:31-len(suffix)] + suffix)
                n += 1
            used_names.add(sheet)
            ws = wb.add_worksheet(sheet)
            ws.freeze_panes(1, 0)
            for j, c in enumerate(cols):
                ws.write_string(0, j, c, header)
                ws.set_column(j, j, 18 if c.endswith("_id") else 24)
            rix = 1
            for row in _iter_slice(parent_job_id, db, ds, off, lim):
                for j, c in enumerate(cols):
                    ws.write_string(rix, j, _safe_excel(row.get(c)), text_fmt)
                rix += 1
            if cols and rix > 1:
                ws.autofilter(0, 0, rix - 1, len(cols) - 1)
        notes = wb.add_worksheet("Read Me")
        notes.write_string(0,0,"Source")
        notes.write_string(0,1,"Persisted Meta raw datasets from parent analysis job; no new Meta API call")
        notes.write_string(1,0,"Parent Job ID")
        notes.write_string(1,1,parent_job_id)
        notes.write_string(2,0,"Note")
        notes.write_string(2,1,"Nested arrays/objects are serialized as JSON text. Formula-like text is prefixed with an apostrophe for Excel safety.")
        notes.set_column(0,0,24); notes.set_column(1,1,100)
        wb.close()
        return Path(path).read_bytes()
    finally:
        try: os.remove(path)
        except OSError: pass


def _dump_jsonl(parent_job_id: str, db: sqlite3.Connection, dataset: str, path: Path):
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in _iter_rows(parent_job_id, db, dataset, []):
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str) + "\n")


def _sqlq(path: Path) -> str:
    return str(path).replace("'", "''")


def _make_duckdb_zip(parent_job_id: str, db: sqlite3.Connection, datasets: list[str], fmt: str) -> bytes:
    import duckdb
    with tempfile.TemporaryDirectory(prefix="elokaby_export_") as td:
        root = Path(td)
        manifest = {
            "parent_job_id": parent_job_id,
            "format": fmt,
            "datasets": [],
            "note": "Generated from persisted parent R2/SQLite data; no Meta API call.",
        }
        con = duckdb.connect(database=":memory:")
        try:
            for ds in datasets:
                jsonl = root / f"{ds}.jsonl"
                _dump_jsonl(parent_job_id, db, ds, jsonl)
                count = _dataset_count(db, ds)
                if count == 0:
                    # Preserve zero-row datasets in the manifest without producing an invalid empty parquet.
                    manifest["datasets"].append({"name": ds, "row_count": 0, "file": None})
                    continue
                src = _sqlq(jsonl)
                if fmt == "parquet_zip":
                    out = root / f"{ds}.parquet"
                    dst = _sqlq(out)
                    con.execute(
                        f"COPY (SELECT * FROM read_json_auto('{src}', format='newline_delimited', union_by_name=true, maximum_object_size=104857600)) "
                        f"TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                else:
                    out = root / f"{ds}.csv"
                    dst = _sqlq(out)
                    con.execute(
                        f"COPY (SELECT * FROM read_json_auto('{src}', format='newline_delimited', union_by_name=true, maximum_object_size=104857600)) "
                        f"TO '{dst}' (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '" + '"' + "', ESCAPE '" + '"' + "')"
                    )
                manifest["datasets"].append({"name": ds, "row_count": count, "file": out.name})
                try: jsonl.unlink()
                except OSError: pass
        finally:
            con.close()
        (root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        zip_path = root / ("El_Okaby_Raw_Parquet.zip" if fmt == "parquet_zip" else "El_Okaby_Raw_CSV.zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            for p in root.iterdir():
                if p == zip_path or p.suffix == ".jsonl":
                    continue
                z.write(p, arcname=p.name)
        return zip_path.read_bytes()


def run_export_job(job: dict):
    params = (job.get("input") or {}).get("params") or {}
    parent_job_id = str(params.get("parent_job_id") or "")
    if not parent_job_id:
        raise ValueError("parent_job_id is required")
    fmt = str(params.get("format") or "parquet_zip")
    if fmt not in {"parquet_zip", "csv_zip", "xlsx_bundle", "xlsx_part"}:
        raise ValueError("Unsupported export format")
    path = _load_parent(parent_job_id)
    db = None
    try:
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        requested = params.get("datasets")
        if requested is not None and (not isinstance(requested, list) or len(requested) > 20):
            raise ValueError("datasets must be an array with at most 20 items")
        datasets = _dataset_names(db, requested)
        if fmt in {"parquet_zip", "csv_zip"}:
            payload = _make_duckdb_zip(parent_job_id, db, datasets, fmt)
            return {
                "parent_job_id": parent_job_id,
                "format": fmt,
                "datasets": datasets,
                "manifest": _manifest(db),
                "note": "Full stored raw export; no new Meta API call.",
            }, payload, ZIP_MIME, False
        if fmt == "xlsx_bundle":
            # Intended for the smaller datasets. Keep a hard total-row bound so the report remains downloadable.
            total = sum(_dataset_count(db, ds) for ds in datasets)
            if total > 120_000:
                raise ValueError("xlsx_bundle is limited to 120000 total rows; export large datasets with xlsx_part")
            payload = _write_xlsx(parent_job_id, db, datasets, {ds:(0,None) for ds in datasets})
            return {
                "parent_job_id": parent_job_id,
                "format": fmt,
                "datasets": datasets,
                "rows_exported": total,
                "note": "Excel-safe raw export; no new Meta API call.",
            }, payload, XLSX_MIME, False
        dataset = str(params.get("dataset") or "")
        if not dataset:
            raise ValueError("dataset is required for xlsx_part")
        if dataset not in _dataset_names(db, [dataset]):
            raise ValueError("Unknown dataset")
        offset = max(0, int(params.get("offset") or 0))
        limit = max(1, min(75_000, int(params.get("limit") or 50_000)))
        total = _dataset_count(db, dataset)
        actual = max(0, min(limit, total - offset))
        payload = _write_xlsx(parent_job_id, db, [dataset], {dataset:(offset, actual)})
        return {
            "parent_job_id": parent_job_id,
            "format": fmt,
            "dataset": dataset,
            "offset": offset,
            "rows_exported": actual,
            "dataset_row_count": total,
            "next_offset": (offset + actual) if offset + actual < total else None,
            "note": "Excel-safe raw export part; no new Meta API call.",
        }, payload, XLSX_MIME, False
    finally:
        try:
            if db is not None: db.close()
        except Exception: pass
        try: os.remove(path)
        except OSError: pass
