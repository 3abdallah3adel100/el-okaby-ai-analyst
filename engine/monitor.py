"""Opt-in independent monitoring; deterministic, explicit Meta action type, deduped alerts."""
import datetime as dt
import hashlib
import json
import os
import urllib.error
import urllib.request
from .meta import MetaClient,parse_tokens
from .analysis import summarize
from .runner import gateway
from .whatsapp import send_template

def monitor():
 if os.getenv('ENABLE_MONITORING','false').lower()!='true':print('Monitoring disabled');return
 template=os.getenv('ALERT_TEMPLATE_NAME','')
 if not template:raise ValueError('Proactive WhatsApp requires approved ALERT_TEMPLATE_NAME')
 action=os.getenv('ALERT_ACTION_TYPE','').strip()
 if not action:raise ValueError('Set ALERT_ACTION_TYPE explicitly; no generic Results aggregation')
 recipient=os.getenv('ALERT_RECIPIENT','').strip()
 if not recipient.isdigit():raise ValueError('Set a private ALERT_RECIPIENT')
 c=MetaClient(parse_tokens(),version=os.getenv('META_GRAPH_VERSION','v26.0'))
 today=dt.date.today(); start=today-dt.timedelta(days=7)
 p={'since':start.isoformat(),'until':today.isoformat(),'level':'account','time_increment':'1','fields':['account_id','date_start','spend','actions'],'action_type':action,'row_limit':5000}
 d=c.insights(p)
 if not d['complete']:
  print('Monitor input incomplete; suppressing metric-change alerts');return
 by={}
 for r in d['rows']:
  aid=r.get('account_id','');by.setdefault(aid,[]).append(r)
 baseline_min=float(os.getenv('ALERT_MIN_SPEND_EGP','500'));threshold=float(os.getenv('ALERT_CPL_MULTIPLIER','1.5'))
 today_str=today.isoformat()
 for aid,rows in by.items():
  current=summarize({'rows':[r for r in rows if r.get('date_start')==today_str],'complete':True},['account_id'],action)
  history=summarize({'rows':[r for r in rows if r.get('date_start')!=today_str],'complete':True},['account_id'],action)
  if not current['groups'] or not history['groups']:continue
  a=current['groups'][0];b=history['groups'][0]
  if a['spend']<baseline_min or b['results']<10 or not b['cost_per_result']:continue
  signal=(not a['results'] and a['spend']>=baseline_min) or (a['cost_per_result'] and a['cost_per_result']>=b['cost_per_result']*threshold)
  if not signal:continue
  key=hashlib.sha256((today_str+':'+aid+':'+action).encode()).hexdigest()
  claimed=gateway('/internal/alerts/claim','POST',{'fingerprint':key,'cooldown_seconds':86400})
  if not claimed.get('send'):continue
  send_template(recipient,template,os.getenv('ALERT_TEMPLATE_LANGUAGE','en_US'),f'El Okaby Alert | Account {aid} | Today spend: {a["spend"]} EGP | {action}: {a["results"]} | cost: {a["cost_per_result"]} | Prior 7d cost: {b["cost_per_result"]}. Verify in Ads Manager.')
 print('Monitoring run finished; no public report output.')
if __name__=='__main__':monitor()

def daily_summary():
 if os.getenv('ENABLE_DAILY_SUMMARY','false').lower()!='true':print('Daily summary disabled');return
 template=os.getenv('ALERT_TEMPLATE_NAME','')
 if not template:raise ValueError('Proactive WhatsApp requires approved ALERT_TEMPLATE_NAME')
 action=os.getenv('ALERT_ACTION_TYPE','').strip();recipient=os.getenv('ALERT_RECIPIENT','').strip()
 if not action or not recipient.isdigit():raise ValueError('Provide ALERT_ACTION_TYPE and ALERT_RECIPIENT')
 c=MetaClient(parse_tokens(),version=os.getenv('META_GRAPH_VERSION','v26.0'))
 d=c.insights({'date_preset':'today','level':'account','fields':['account_id','account_name','spend','impressions','clicks','actions'],'row_limit':3000,'action_type':action})
 a=summarize(d,group_by=['account_id'],action_type=action,top_n=100)
 all_rows=summarize(d,group_by=[],action_type=action,top_n=100)
 # Group-by empty uses single total grouping.
 total=all_rows['groups'][0] if all_rows['groups'] else None
 if not total:send_template(recipient,template,os.getenv('ALERT_TEMPLATE_LANGUAGE','en_US'),'El Okaby Daily Summary: No data returned. Check account errors.');return
 text=f'El Okaby Daily Summary (Meta data at {d["live_at_utc"]})\nSpend: {total["spend"]} EGP\n{action}: {total["results"]}\nCost per {action}: {total["cost_per_result"]}\nAccounts: {len(a["groups"])}; complete: {d["complete"]}; errors: {len(d["account_errors"])}.\nMeta conversion reporting may lag.'
 send_template(recipient,template,os.getenv('ALERT_TEMPLATE_LANGUAGE','en_US'),text)
