import datetime as dt
import os
import tempfile
from unittest.mock import patch

from engine.generic_job import GenericDB
from engine import repair_job


def _tmpdb():
    fd, path = tempfile.mkstemp(suffix='.sqlite3')
    os.close(fd)
    return path, GenericDB(path)


def test_creative_repair_falls_back_to_small_pages():
    path, db = _tmpdb()
    try:
        spec = {
            'name': 'adcreatives', 'source': 'adcreatives', 'fields': ['id','name'],
            'account_ids': [], 'row_limit': 100000, 'page_limit': 1000,
            'time_range': {'mode':'preset','date_preset':'maximum'}, 'breakdowns': [],
            'level':'ad', 'time_increment':None, 'filtering':[], 'limit':200,
        }
        db.ensure_dataset(spec)
        db.set_progress('adcreatives','123',None,'failed')
        calls=[]
        def fake_collect(_client, trial, _account):
            calls.append(trial['limit'])
            if trial['limit'] > 5:
                raise RuntimeError('Please reduce the amount of data')
            return [
                {'id':'c1','name':'one','account_id':'123'},
                {'id':'c2','name':'two','account_id':'123'},
            ]
        with patch.object(repair_job, '_collect_pages', fake_collect), patch.object(repair_job, '_progress', lambda *a, **k: None):
            ok, inserted, err = repair_job._repair_creative_account(
                'child', db, object(), spec, {'id':'123'}, '123'
            )
        assert ok is True
        assert inserted == 2
        assert err is None
        assert calls == [25,10,5]
        assert db.progress('adcreatives','123')[1] == 'done'
        assert db.dataset_count('adcreatives') == 2
    finally:
        db.close()
        os.remove(path)


def test_daily_repair_splits_failed_30_day_window_to_7_day_segments():
    path, db = _tmpdb()
    try:
        spec = {
            'name':'ad_daily','source':'insights','fields':['account_id','ad_id','spend'],
            'account_ids':[],'row_limit':1000000,'page_limit':1000,
            'time_range':{'mode':'maximum','date_preset':'maximum'},'breakdowns':[],
            'level':'ad','time_increment':'1','filtering':[],'limit':200,
        }
        db.ensure_dataset(spec)
        scope='123:2026-01-01:2026-01-30'
        db.set_progress('ad_daily',scope,None,'failed')
        attempts=[]
        def fake_fetch(_client, _spec, _account, since, until):
            attempts.append((since,until))
            a=dt.date.fromisoformat(since); b=dt.date.fromisoformat(until)
            if (b-a).days+1 > 7:
                raise RuntimeError('temporary service error')
            return [{
                'account_id':'123','campaign_id':'c','adset_id':'s','ad_id':'a'+since,
                'date_start':since,'date_stop':until,'spend':'1.0'
            }]
        counter={'n':0}
        def fake_upload(_parent, dbx, sp, scope_id, account_id, rows, **kwargs):
            counter['n'] += 1
            key=f"shards/parent/ad_daily/123/fake-{counter['n']}.jsonl.gz"
            dbx.add_shard(
                dataset='ad_daily', shard_key=key, scope_id=scope_id, account_id=account_id,
                row_count=len(rows), date_start=rows[0]['date_start'], date_stop=rows[-1]['date_stop'],
                raw_bytes=100, gzip_bytes=50, increment_dataset=True,
            )
            return key, len(rows)
        with patch.object(repair_job, '_remove_scope_shards', lambda *a, **k: 0), \
             patch.object(repair_job, '_clear_scope_errors', lambda *a, **k: None), \
             patch.object(repair_job, '_progress', lambda *a, **k: None), \
             patch.object(repair_job, '_fetch_daily_segment', fake_fetch), \
             patch.object(repair_job, '_upload_rows_shard', fake_upload):
            ok, repaired, failures = repair_job._repair_daily_scope(
                'child','parent',db,object(),spec,{'id':'123'},scope,
                '2026-01-01','2026-01-30',set()
            )
        assert ok is True
        assert failures == []
        assert repaired == 5  # 7+7+7+7+2 day chunks
        assert db.progress('ad_daily',scope)[1] == 'done'
        assert db.dataset_count('ad_daily') == 5
        assert attempts[0] == ('2026-01-01','2026-01-30')
        assert len(attempts) == 6
    finally:
        db.close()
        os.remove(path)
