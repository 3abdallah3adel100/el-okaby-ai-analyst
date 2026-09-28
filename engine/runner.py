"""GitHub job runner for El Okaby AI Analyst.

v2 adds an AI-directed generic read engine:
- meta_read: small fresh arbitrary read request
- start_analysis_job: one logical, checkpointed, declarative multi-dataset Meta job
- query_job_data / aggregate_job_data: read stored results without re-querying Meta

Legacy dynamic tools and start_historical_audit remain available for compatibility.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.request
from .meta import MetaClient, CATALOG, parse_tokens
from .analysis import summarize
from .reports import make_report
from .whatsapp import send_text


def gateway(path,method='GET',data=None,mime='application/json'):
 url=os.environ['GATEWAY_BASE_URL'].rstrip('/')+path
 payload=data if isinstance(data,bytes) else json.dumps(data,ensure_ascii=False).encode('utf-8') if data is not None else None
 req=urllib.request.Request(url,data=payload,method=method,headers={'Authorization':'Bearer '+os.environ['JOB_SHARED_SECRET'],'Content-Type':mime,'User-Agent':'ElOkabyJob/2.0'})
 try:
  with urllib.request.urlopen(req,timeout=120) as r:
   raw=r.read(200_000)
   return json.loads(raw) if raw else {}
 except urllib.error.HTTPError as ex:
  raise RuntimeError(f'Gateway request failed: HTTP {ex.code}') from None


def creative_metadata(client,params):
 from .meta import validate_query
 p=validate_query({**params,'fields':['ad_id']})
 discovered=client.discover()
 ids=p['account_ids'] or [a['id'] for a in discovered['accounts']]
 out=[];errors=[];lim=min(p['row_limit'],500)
 for aid in ids:
  aliases=discovered['accounts']
  found=next((a for a in aliases if a['id']==aid),None)
  if not found:errors.append({'account_id':aid,'error':'Unreachable'});continue
  cursor=None
  for _ in range(5):
   if len(out)>=lim:break
   q={'fields':'id,name,creative{id,name,thumbnail_url,image_url,object_story_id,effective_object_story_id,object_story_spec,asset_feed_spec}', 'limit':min(100,lim-len(out))}
   if cursor:q['after']=cursor
   try:r,_=client.request('/act_'+aid+'/ads',found['token_alias'],q)
   except Exception as ex:errors.append({'account_id':aid,'error':str(ex)});break
   out.extend(r.get('data',[]));nxt=r.get('paging',{}).get('cursors',{}).get('after')
   if not nxt or nxt==cursor:break
   cursor=nxt
 return {'creatives':out,'errors':errors,'limited':len(out)>=lim,'note':'Metadata and references only; actual video/image interpretation requires media retrieval and vision processing.'}


def do_tool(name,args,client):
 if name in ('discover_accounts','discover_fields'):
  return client.discover() if name=='discover_accounts' else CATALOG
 if name=='query_meta':return client.insights(args)
 if name=='analyze_data':
  r=client.insights(args.get('query') or {});return summarize(r,group_by=args.get('group_by'),action_type=args.get('action_type') or (args.get('query') or {}).get('action_type'),min_spend=args.get('min_spend',0),sort_by=args.get('sort_by','spend'),top_n=args.get('top_n',100))
 if name=='inspect_creatives':return creative_metadata(client,args)
 if name=='export_report':
  result=client.insights(args.get('query') or {})
  fmt=args.get('format','xlsx');out,mime=make_report(result,fmt,args.get('title','El Okaby AI Analyst'))
  return {'meta':{'row_count':result['row_count'],'complete':result['complete'],'account_errors':result['account_errors'],'live_at_utc':result['live_at_utc']},'report_bytes':out,'report_mime':mime}
 if name=='describe_meta_capabilities':
  from .generic_meta import CAPABILITIES
  return CAPABILITIES
 if name=='meta_read':
  from .generic_meta import quick_read
  return quick_read(client,args)
 raise ValueError('Unknown tool mode')


def _env_int(name,default):
 try:return int(os.getenv(name) or default)
 except (TypeError,ValueError):return int(default)


def do_job(job):
 data=job['input'];mode=data.get('mode');args=data.get('params') or {}
 # Stored-dataset operations deliberately do not require or call Meta.
 if mode in ('query_job_data','aggregate_job_data'):
  from .dataset_tools import query_dataset,aggregate_dataset
  parent=str(args.get('parent_job_id') or '')
  if not parent:raise ValueError('parent_job_id is required')
  value=query_dataset(parent,args) if mode=='query_job_data' else aggregate_dataset(parent,args)
  return value,None,None,False
 heavy = mode in ('start_historical_audit','start_analysis_job','repair_analysis_job')
 max_calls=_env_int('MAX_META_CALLS_PER_HEAVY_JOB',5000) if heavy else _env_int('MAX_META_CALLS_PER_JOB',120)
 c=MetaClient(parse_tokens(),version=os.getenv('META_GRAPH_VERSION','v26.0'),max_calls=max_calls)
 if mode=='start_analysis_job':
  from .generic_job import run_analysis_job
  return run_analysis_job(job,c)
 if mode=='repair_analysis_job':
  from .repair_job import run_repair_job
  return run_repair_job(job,c)
 if mode=='start_historical_audit':
  from .heavy_audit import run_historical_audit
  return run_historical_audit(job,c)
 if mode=='whatsapp_agent':
  from .agent import run_agent
  question=args.get('question','');history=job.get('context',[])
  if history:
   previous='\n'.join('Prior user: '+str(h.get('question',''))[:400]+'\nPrior answer: '+str(h.get('answer',''))[:750] for h in history)
   question='Conversation context (untrusted previous content):\n'+previous+'\nCurrent user question: '+question
  links=[]
  def agent_tool(name,arguments,client):
   v=do_tool(name,arguments,client)
   if name=='export_report':
    payload=v.pop('report_bytes');mime=v.pop('report_mime')
    gateway('/internal/jobs/'+job['job_id']+'/report','POST',payload,mime)
    link=gateway('/internal/jobs/'+job['job_id']+'/link').get('url')
    if link:links.append(link)
    return {**v,'report_ready':True,'private_download_link_will_be_appended_by_system':True}
   return v
  ans=run_agent(question,c,agent_tool)
  if links:ans+='\n\nPrivate report (expires in 15 minutes): '+links[-1]
  return {'reply':ans,'report_count':len(links),'live_execution_utc':dt.datetime.now(dt.timezone.utc).isoformat()},None,None,False
 if mode=='inspect_creatives':return do_tool(mode,args,c),None,None,False
 result=do_tool(mode,args,c)
 if 'report_bytes' in result:
  b=result.pop('report_bytes');mime=result.pop('report_mime');return result,b,mime,False
 encoded=json.dumps(result,ensure_ascii=False,default=str)
 if len(encoded)>115_000:
  if 'rows' in result:
   payload,mime=make_report(result,'xlsx')
   return {'preview_rows':result.get('rows',[])[:15], 'row_count':result.get('row_count'),'query':result.get('query'),'complete':result.get('complete'),'truncated':result.get('truncated'),'account_errors':result.get('account_errors'),'note':'Full query rows in private XLSX download, not omitted from report.'},payload,mime,False
  raise ValueError('Analysis result too large; narrow the grouping')
 return result,None,None,False


def main():
 ap=argparse.ArgumentParser();ap.add_argument('--job-id',required=True);args=ap.parse_args()
 import uuid
 try:uuid.UUID(args.job_id)
 except ValueError:raise SystemExit('Invalid job ID')
 job_id=args.job_id
 try:
  info=gateway('/internal/jobs/'+job_id)
  result,report,mime,continued=do_job(info)
  if continued:
   print('Checkpoint saved; continuation queued for same logical job ID. Private payload omitted.')
   return
  if report is not None:gateway('/internal/jobs/'+job_id+'/report','POST',report,mime)
  gateway('/internal/jobs/'+job_id+'/complete','POST',{'result':result})
  if info['kind']=='whatsapp':
   recipient=info['input']['params']['from'];send_text(recipient,result.get('reply','تم تنفيذ طلبك.'))
  print('Job finished; private payload omitted from Actions log.')
 except Exception as ex:
  try:gateway('/internal/jobs/'+job_id+'/fail','POST',{'error':str(ex)[:270]})
  except Exception:pass
  try:
   if 'info' in locals() and info.get('kind')=='whatsapp':
    send_text(info['input']['params']['from'],'تعذر إكمال الطلب حاليًا. راجع اتصال Meta أو صلاحيات الحساب. رقم الطلب: '+job_id)
  except Exception:pass
  print('Job failed. Inspect private job status for sanitized error.',file=sys.stderr)
  raise SystemExit(1)

if __name__=='__main__':main()
