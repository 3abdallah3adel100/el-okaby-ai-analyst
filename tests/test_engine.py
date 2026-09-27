import datetime as dt
import json
import os
import unittest
from unittest.mock import patch
from decimal import Decimal
from engine.meta import validate_query,MetaClient,MetaError,allowlist,chunks
from engine.analysis import summarize,action_count
from engine.reports import make_report,safe_cell

ENV={'META_ACCOUNT_ALLOWLIST':'100000,200000','META_ACCOUNT_TOKEN_MAP_JSON':'{"100000":"a","200000":"b"}'}
class ValidateTests(unittest.TestCase):
 def test_dynamic_arbitrary_field_and_lifetime(self):
  with patch.dict(os.environ,ENV):
   q=validate_query({'fields':['spend','actions','purchase_roas'],'date_preset':'maximum','level':'ad'})
   self.assertEqual(q['account_ids'],['100000','200000']);self.assertEqual(q['fields'][2],'purchase_roas')
 def test_cross_account_denied(self):
  with patch.dict(os.environ,ENV):
   with self.assertRaises(ValueError):validate_query({'account_ids':['999999']})
 def test_invalid_fields_and_dates(self):
  with patch.dict(os.environ,ENV):
   for p in ({'fields':['spend; DROP TABLE']},{'since':'2026-10-05','until':'2026-01-01'},{'row_limit':15000}):
    with self.assertRaises(ValueError):validate_query(p)
 def test_date_chunks(self):
  c=list(chunks(dt.date(2026,1,1),dt.date(2026,2,1)))
  self.assertEqual(c,[('2026-01-01','2026-01-28'),('2026-01-29','2026-02-01')])
 def test_required_allowlist(self):
  with patch.dict(os.environ,{'META_ACCOUNT_ALLOWLIST':''}):
   with self.assertRaises(ValueError):allowlist()
 def test_weighted_action_type_and_no_generic_cpl(self):
  r={'complete':True,'rows':[
   {'account_id':'100000','spend':'60','impressions':'100','clicks':'10','actions':[{'action_type':'onsite_conversion.messaging_conversation_started_7d','value':'3'},{'action_type':'lead','value':'10'}]},
   {'account_id':'100000','spend':'40','impressions':'100','clicks':'10','actions':[{'action_type':'onsite_conversion.messaging_conversation_started_7d','value':'1'}]}
  ]}
  a=summarize(r,action_type='onsite_conversion.messaging_conversation_started_7d');self.assertEqual(a['groups'][0]['cost_per_result'],25)
  b=summarize(r);self.assertIsNone(b['groups'][0]['cost_per_result']);self.assertIsNone(b['groups'][0]['results'])
 def test_total_spend_across_accounts_without_grouping(self):
  r={'rows':[{'account_id':'100000','spend':'40','actions':[{'action_type':'lead','value':'2'}]},{'account_id':'200000','spend':'60','actions':[{'action_type':'lead','value':'3'}]}],'complete':True}
  t=summarize(r,group_by=[],action_type='lead')
  self.assertEqual(len(t['groups']),1);self.assertEqual(t['groups'][0]['spend'],100);self.assertEqual(t['groups'][0]['cost_per_result'],20)
 def test_partial_not_zero(self):
  a=summarize({'complete':False,'account_errors':[{'account_id':'200000','error':'403'}],'rows':[{'account_id':'100000','spend':'100'}]})
  self.assertTrue(a['partial_or_incomplete']);self.assertEqual(a['account_errors'][0]['account_id'],'200000')
 def test_csv_formula_escape(self):
  self.assertEqual(safe_cell('=HYPERLINK("x")'),"'=HYPERLINK(\"x\")")
  b,_=make_report({'rows':[{'ad_name':'=HYPERLINK("bad")'}]},'csv');self.assertIn(b"'=HYPERLINK",b)
 def test_xlsx_pdf_runtime(self):
  result={'rows':[{'account_id':'100000','spend':'123.4','actions':[]}],'complete':True,'live_at_utc':'2026-09-28T00:00:00+00:00'}
  try:
   x,_=make_report(result,'xlsx');p,_=make_report(result,'pdf')
  except ImportError:self.skipTest('Optional runtime dependencies not installed locally')
  self.assertTrue(x.startswith(b'PK'));self.assertTrue(p.startswith(b'%PDF'))
 def test_meta_pagination_and_errors(self):
  class Fake(MetaClient):
   def __init__(self):super().__init__({'a':'fake-long-test-token-example'})
   def discover(self):return {'accounts':[{'id':'100000','token_alias':'a'}],'errors':[]}
   def request(self,path,alias,params=None):
    if path.endswith('/insights'):
     if params.get('after')=='cursor':return {'data':[{'spend':'2'}]},{}
     return {'data':[{'spend':'1'}],'paging':{'cursors':{'after':'cursor'}}},{}
    raise AssertionError('unexpected path')
  with patch.dict(os.environ,ENV):
   x=Fake().insights({'account_ids':['100000'],'date_preset':'today','fields':['spend']})
  self.assertEqual(len(x['rows']),2);self.assertTrue(x['complete'])
 def test_discovery_partial(self):
  class Fake(MetaClient):
   def __init__(self):super().__init__({'a':'fake-long-test-token-example'})
   def discover(self):return {'accounts':[{'id':'100000','token_alias':'a'}],'errors':[]}
   def request(self,path,alias,params=None):return {'data':[{'spend':'1'}]},{}
  with patch.dict(os.environ,ENV):
   x=Fake().insights({'account_ids':['100000','200000'],'fields':['spend']})
  self.assertFalse(x['complete']);self.assertEqual(x['account_errors'][0]['account_id'],'200000')
if __name__=='__main__':unittest.main()
