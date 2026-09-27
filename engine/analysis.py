"""Deterministic accounting, always requiring explicit result/action type."""
from __future__ import annotations
from collections import defaultdict
from decimal import Decimal, InvalidOperation

def money(value):
 try:return Decimal(str(value or '0'))
 except (InvalidOperation,ValueError):return Decimal('0')

def action_count(row,action_type):
 if not action_type:return None
 total=Decimal('0')
 for a in row.get('actions',[]) or []:
  if a.get('action_type')==action_type:total+=money(a.get('value'))
 return total

def summarize(result,group_by=None,action_type=None,min_spend=0,sort_by='spend',top_n=100):
 group_by=list(['account_id'] if group_by is None else group_by)
 if len(group_by)>4 or any(not isinstance(k,str) or k.startswith('_') or len(k)>90 for k in group_by):raise ValueError('Invalid group_by')
 if action_type and (not isinstance(action_type,str) or len(action_type)>100):raise ValueError('Invalid action_type')
 groups=defaultdict(lambda:{'spend':Decimal(0),'impressions':Decimal(0),'clicks':Decimal(0),'result_count':Decimal(0),'row_count':0})
 for r in result.get('rows',[]):
  key=tuple(str(r.get(k,'')) for k in group_by)
  a=groups[key];a['spend']+=money(r.get('spend'));a['impressions']+=money(r.get('impressions'));a['clicks']+=money(r.get('clicks'))
  n=action_count(r,action_type)
  if n is not None:a['result_count']+=n
  a['row_count']+=1
 records=[]
 for key,a in groups.items():
  if a['spend']<money(min_spend):continue
  count=a['result_count']
  records.append({'group':dict(zip(group_by,key)),'spend':round(float(a['spend']),2),'impressions':int(a['impressions']),'clicks':int(a['clicks']),'result_type':action_type,'results':round(float(count),2) if action_type else None,'cost_per_result':round(float(a['spend']/count),2) if action_type and count>0 else None,'ctr_pct':round(float(a['clicks']*100/a['impressions']),3) if a['impressions'] else None,'row_count':a['row_count']})
 if sort_by not in ['spend','impressions','clicks','results','cost_per_result','ctr_pct']:raise ValueError('Invalid sort_by')
 records.sort(key=lambda x: (x[sort_by] is None, x[sort_by] if x[sort_by] is not None else -1),reverse=sort_by!='cost_per_result')
 return {'groups':records[:int(top_n)],'group_count':len(records),'truncated_groups':len(records)>int(top_n),'result_type':action_type,'note':'No generic Leads/Results CPL is computed. Specify a single Meta action_type. Reach/frequency are intentionally not summed. Action deduplication and attribution follow Meta reporting.','partial_or_incomplete':not result.get('complete',False),'account_errors':result.get('account_errors',[]),'query':result.get('query'),'live_at_utc':result.get('live_at_utc')}
