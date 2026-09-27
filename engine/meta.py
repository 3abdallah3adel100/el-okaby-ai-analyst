"""Read-only, dynamically parameterized Meta Marketing API client.

No stored report shape, objective-specific hardcoded report, or guessed generic 'results' CPL.
Live queries use the API at execution time; attribution delays may still apply.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

NAME = re.compile(r'^[A-Za-z][A-Za-z0-9_]{0,90}$')
ACCOUNT = re.compile(r'^(?:act_)?(\d{3,30})$')
LEVELS = {'account','campaign','adset','ad'}
PRESETS = {'today','yesterday','this_week_mon_today','this_week_sun_today','last_week_mon_sun','last_week_sun_sat','last_3d','last_7d','last_14d','last_28d','last_30d','last_90d','this_month','last_month','this_quarter','last_quarter','this_year','last_year','maximum','data_maximum'}
DEFAULT_FIELDS = ['account_id','account_name','date_start','date_stop','spend','impressions','clicks','actions','cost_per_action_type']
CATALOG = {
  'levels':sorted(LEVELS),
  'example_fields':['account_id','account_name','campaign_id','campaign_name','adset_id','adset_name','ad_id','ad_name','objective','optimization_goal','spend','impressions','reach','frequency','clicks','inline_link_clicks','cpc','cpm','ctr','actions','cost_per_action_type','action_values','purchase_roas','date_start','date_stop'],
  'example_breakdowns':['age','gender','country','region','placement','publisher_platform','platform_position','device_platform','hourly_stats_aggregated_by_advertiser_time_zone','action_type'],
  'metric_note':'The lists are examples, not a frozen report. Meta validates fields, compatible breakdowns and action types at request time. Never mix action types in one CPL.',
  'action_note':'Set action_type explicitly for cost per result. WhatsApp messaging events and lead form submissions are different types. Inspect raw actions before selection.',
  'freshness':'Fresh API request upon job execution; Meta may report attributed results later.'
}

class MetaError(Exception):
 def __init__(self,message:str,code:Any=None):
  super().__init__(message); self.code=code

def clean_account(a:Any)->str:
 m=ACCOUNT.fullmatch(str(a).strip())
 if not m:raise ValueError('Invalid ad account ID')
 return m.group(1)

def parse_tokens() -> dict[str,str]:
 """Use a single private GitHub Secret JSON; aliases are never printed."""
 raw=os.getenv('META_TOKENS_JSON','')
 if not raw:raise ValueError('META_TOKENS_JSON is missing')
 obj=json.loads(raw)
 if not isinstance(obj,dict) or not obj:raise ValueError('META_TOKENS_JSON must map aliases to access tokens')
 if any(not NAME.fullmatch(str(k)) or not isinstance(v,str) or len(v)<20 for k,v in obj.items()):raise ValueError('Invalid token alias or missing token value')
 return obj

def allowlist()->list[str]:
 """Optional owner-specified scope; empty means all accounts *actually exposed by the tokens*."""
 ids=[clean_account(a) for a in os.getenv('META_ACCOUNT_ALLOWLIST','').split(',') if a.strip()]
 return sorted(set(ids))

def account_token_map()->dict[str,str]:
 raw=os.getenv('META_ACCOUNT_TOKEN_MAP_JSON','{}')
 obj=json.loads(raw)
 if not isinstance(obj,dict):raise ValueError('Invalid META_ACCOUNT_TOKEN_MAP_JSON')
 return {clean_account(k):v for k,v in obj.items()}

def _name_list(v:Any,max_n=32)->list[str]:
 if v is None:return []
 if not isinstance(v,list) or len(v)>max_n or any(not isinstance(x,str) or not NAME.fullmatch(x) for x in v):raise ValueError('Invalid field or breakdown list')
 return list(dict.fromkeys(v))

def _date(v:Any)->dt.date:
 try:return dt.date.fromisoformat(str(v))
 except ValueError:raise ValueError('Dates must use YYYY-MM-DD')

def chunks(s:dt.date,e:dt.date,days:int=28):
 while s<=e:
  stop=min(s+dt.timedelta(days=days-1),e)
  yield s.isoformat(),stop.isoformat();s=stop+dt.timedelta(days=1)

def validate_query(p:dict)->dict:
 if not isinstance(p,dict):raise ValueError('Query must be an object')
 level=p.get('level','ad')
 if level not in LEVELS:raise ValueError('Invalid Meta Insights level')
 fields=_name_list(p.get('fields') or DEFAULT_FIELDS)
 if not fields:raise ValueError('At least one field required')
 breakdowns=_name_list(p.get('breakdowns',[]),3)
 rawids=p.get('account_ids',[])
 if rawids is None:rawids=[]
 if not isinstance(rawids,list) or len(rawids)>100:raise ValueError('Invalid account selection')
 permitted=set(allowlist()); requested=list(dict.fromkeys(clean_account(i) for i in rawids)) if rawids else sorted(permitted)
 if permitted and not set(requested)<=permitted:raise ValueError('Account outside configured optional account scope')
 # No manual scope: an empty request means 'discover all accounts this token can access'.
 # MetaClient.insights() resolves it against real discovery before making any Insights call.
 since=p.get('since');until=p.get('until');preset=p.get('date_preset')
 if bool(since)!=bool(until):raise ValueError('Both since and until are required')
 if since:
  a,b=_date(since),_date(until)
  if a>b or b>dt.date.today()+dt.timedelta(days=1):raise ValueError('Invalid date range')
  if preset:raise ValueError('Use date range OR date_preset')
 elif preset and preset not in PRESETS:raise ValueError('Unsupported date_preset')
 else:preset=preset or 'today'
 row_limit=int(p.get('row_limit',3000))
 if row_limit<1 or row_limit>10000:raise ValueError('row_limit must be 1..10000; requests are bounded')
 increment=p.get('time_increment')
 if increment is not None and str(increment) not in ['all_days','1','7','monthly']:raise ValueError('Unsupported time_increment')
 filters=p.get('filtering',[])
 if not isinstance(filters,list) or len(filters)>10 or len(json.dumps(filters))>3000:raise ValueError('Too many filters')
 for f in filters:
  if not isinstance(f,dict) or set(f)-{'field','operator','value'} or not NAME.fullmatch(str(f.get('field',''))) or f.get('operator') not in ['EQUAL','NOT_EQUAL','GREATER_THAN','LESS_THAN','IN','NOT_IN','CONTAIN','NOT_CONTAIN']:
   raise ValueError('Unsupported Meta filtering structure')
 action_type=p.get('action_type')
 if action_type is not None and (not isinstance(action_type,str) or not re.fullmatch(r'[a-zA-Z0-9_.]{1,100}',action_type)):raise ValueError('Invalid action_type')
 return dict(account_ids=requested,level=level,fields=fields,breakdowns=breakdowns,since=str(since) if since else None,until=str(until) if until else None,date_preset=preset,time_increment=str(increment) if increment is not None else None,filtering=filters,row_limit=row_limit,action_type=action_type)

@dataclass
class MetaClient:
 tokens:dict[str,str]
 version:str='v26.0'
 max_calls:int=120
 calls:int=0
 def request(self,path:str,alias:str,params:dict|None=None)->tuple[dict,dict]:
  if alias not in self.tokens:raise MetaError('Configured token alias does not exist')
  if self.calls>=self.max_calls:raise MetaError('Per-job API call safety limit reached')
  if not path.startswith('/') or '..' in path or '://' in path:raise MetaError('Invalid API path')
  token=self.tokens[alias]
  q={k:v for k,v in (params or {}).items() if v is not None}
  url='https://graph.facebook.com/'+self.version+path+('?' + urllib.parse.urlencode(q) if q else '')
  headers={'Authorization':'Bearer '+token,'Accept':'application/json','User-Agent':'ElOkabyAIAnalyst/1.0'}
  last='Meta request failed'
  for attempt in range(4):
   self.calls+=1
   req=urllib.request.Request(url,headers=headers)
   try:
    with urllib.request.urlopen(req,timeout=45) as resp:
     payload=json.load(resp)
     usage={k:resp.headers.get(k) for k in ['x-business-use-case-usage','x-app-usage','retry-after'] if resp.headers.get(k)}
     return payload,usage
   except urllib.error.HTTPError as ex:
    try:obj=json.loads(ex.read(2048))
    except Exception:obj={}
    detail=obj.get('error',{}) if isinstance(obj,dict) else {}
    code=detail.get('code');last=f'Meta HTTP {ex.code}: {str(detail.get("message","Request failed"))[:220]}'
    if (ex.code==429 or code in [4,17,32,613,80004]) and attempt<3 and self.calls<self.max_calls:
     wait=min(2**attempt,16)
     try:wait=min(float(ex.headers.get('Retry-After') or 0) or wait,16)
     except (ValueError,TypeError):pass
     time.sleep(wait);continue
    raise MetaError(last,code) from None
   except (urllib.error.URLError,TimeoutError) as ex:
    last='Meta network error';
    if attempt<3 and self.calls<self.max_calls:time.sleep(2**attempt);continue
    raise MetaError(last) from ex
  raise MetaError(last)
 def discover(self)->dict:
  """Discover token-visible accounts, with optional business enrichment and optional scope.

  Accounts are never inferred from numeric IDs supplied by the model. Only Meta-returned
  IDs can be queried; duplicate IDs are merged across tokens. No token values are returned.
  """
  if hasattr(self,'_discovery_cache'):return self._discovery_cache
  allowed=set(allowlist());preferred=account_token_map()
  accounts={};errors=[];warnings=[];usage={};businesses_by_alias={}
  coverage_complete=True
  def add_account(a,alias,source,business_id=None):
   nonlocal accounts
   try:aid=clean_account(a['id'])
   except (KeyError,ValueError,TypeError):
    warnings.append({'token_alias':alias,'source':source,'warning':'Meta returned an invalid account ID'});return
   if allowed and aid not in allowed:return
   if aid not in accounts:
    accounts[aid]={**{k:v for k,v in a.items() if k!='id'},'id':aid,'token_alias':alias,'token_aliases':[alias],'discovered_via':[source]}
    if business_id:accounts[aid]['business_id']=business_id
   else:
    item=accounts[aid]
    if alias not in item['token_aliases']:item['token_aliases'].append(alias)
    if source not in item['discovered_via']:item['discovered_via'].append(source)
    if business_id and 'business_id' not in item:item['business_id']=business_id
    if preferred.get(aid)==alias:item['token_alias']=alias
  def pages(path,alias,fields,source,limit=30,required=False):
   nonlocal coverage_complete
   after=None;seen=set()
   for _ in range(limit):
    try:
     payload,head=self.request(path,alias,{'fields':fields,'limit':100,**({'after':after} if after else {})})
     if head:usage[alias]=head
     for item in payload.get('data',[]):yield item
     page=payload.get('paging',{});nxt=page.get('cursors',{}).get('after')
     # A terminal page has neither a next URL nor a novel cursor.
     if not page.get('next'):break
     if not nxt or nxt==after or nxt in seen:
      coverage_complete=False
      errors.append({'token_alias':alias,'source':source,'error':'Pagination cannot be completed'});break
     seen.add(nxt);after=nxt
    except Exception as ex:
     entry={'token_alias':alias,'source':source,'error':str(ex)[:230]}
     (errors if required else warnings).append(entry)
     if required:coverage_complete=False
     break
   else:
    coverage_complete=False
    errors.append({'token_alias':alias,'source':source,'error':'Pagination safety limit reached'})
  extra=[x.strip() for x in os.getenv('META_BUSINESS_IDS','').split(',') if x.strip()]
  for biz in extra:
   if not biz.isdigit():raise ValueError('META_BUSINESS_IDS must be comma-separated numeric business IDs')
  for alias in self.tokens:
   for a in pages('/me/adaccounts',alias,'id,name,account_status,timezone_name,business{id,name}','me/adaccounts',required=True):
    add_account(a,alias,'me/adaccounts')
   business_ids=set(extra)
   # Best-effort automatic Business discovery; this edge may be unavailable to
   # system-user tokens. /me/adaccounts continues to work if it is denied.
   for b in pages('/me/businesses',alias,'id,name','me/businesses',limit=10):
    bid=str(b.get('id',''))
    if bid.isdigit():business_ids.add(bid)
   businesses_by_alias[alias]=len(business_ids)
   for biz in sorted(business_ids):
    for edge in ('owned_ad_accounts','client_ad_accounts'):
     source=f'{biz}/{edge}'
     for a in pages('/'+biz+'/'+edge,alias,'id,name,account_status,timezone_name',source,limit=20):
      add_account(a,alias,source,biz)
  missing=sorted(allowed-set(accounts)) if allowed else []
  if missing:coverage_complete=False
  if not accounts:coverage_complete=False
  self._discovery_cache={'accounts':sorted(accounts.values(),key=lambda a:a['id']),
   'account_count':len(accounts),'token_count':len(self.tokens),
   'scope':'optional_allowlist' if allowed else 'all_token_accessible_accounts',
   'discovery_complete':coverage_complete,
   'missing_account_ids':missing,'errors':errors,'warnings':warnings,
   'business_count_by_token':businesses_by_alias,
   'api_call_count':self.calls,'usage_headers':usage,
   'live_at_utc':dt.datetime.now(dt.timezone.utc).isoformat()}
  return self._discovery_cache
 def insights(self,params:dict)->dict:
  p=validate_query(params);discovered=self.discover();index={a['id']:a for a in discovered['accounts']}
  if not p['account_ids']:
   p['account_ids']=sorted(index)  # Default: all accounts found through authorized tokens.
  rows=[];failed=[];window_errors=[];usage={};truncated=False
  for aid in p['account_ids']:
   if aid not in index:
    failed.append({'account_id':aid,'error':'No token with account access discovered; not counted as zero'});continue
   alias=index[aid]['token_alias']; windows=list(chunks(_date(p['since']),_date(p['until']))) if p['since'] else [(None,None)]
   for since,until in windows:
    q={'level':p['level'],'fields':','.join(p['fields']),'limit':min(500,p['row_limit']),'breakdowns':','.join(p['breakdowns']) if p['breakdowns'] else None,'filtering':json.dumps(p['filtering']) if p['filtering'] else None,'time_increment':p['time_increment']}
    if since:q['time_range']=json.dumps({'since':since,'until':until})
    else:q['date_preset']=p['date_preset']
    cursor=None
    for _ in range(50):
     try:
      result,head=self.request('/act_'+aid+'/insights',alias,{**q,**({'after':cursor} if cursor else {})})
      if head:usage[aid]=head
      data=result.get('data',[])
      remaining=p['row_limit']-len(rows)
      if remaining<=0:truncated=True;break
      try:agents=json.loads(os.getenv('META_AGENT_MAP_JSON','{}'))
      except json.JSONDecodeError:agents={}
      for item in data[:remaining]:
       item.setdefault('account_id',aid)
       if 'account_id' in item and isinstance(agents,dict):item['agent']=agents.get(clean_account(item['account_id']),'Unmapped')
       rows.append(item)
      
      if len(data)>remaining:truncated=True;break
      new_cursor=result.get('paging',{}).get('cursors',{}).get('after')
      if not new_cursor or new_cursor==cursor:break
      cursor=new_cursor
     except MetaError as ex:
      window_errors.append({'account_id':aid,'since':since,'until':until,'error':str(ex)});break
    else:truncated=True
    if truncated:break
   if truncated:break
  return {'rows':rows,'requested_account_ids':p['account_ids'],'account_errors':failed+window_errors,'discovery_errors':discovered['errors'],'row_count':len(rows),'truncated':truncated,'complete':bool(p['account_ids']) and discovered.get('discovery_complete',False) and not(truncated or failed or window_errors),'discovery_complete':discovered.get('discovery_complete',False),'discovered_account_count':len(index),'discovery_warnings':discovered.get('warnings',[]),'api_call_count':self.calls,'usage_headers':usage,'query':p,'live_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'freshness_note':'Requested from Meta during this job; action attribution may lag.'}
