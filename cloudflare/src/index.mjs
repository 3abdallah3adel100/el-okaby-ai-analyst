import {json,err,uuid,randomToken,sha256,timingEqual,hmacVerify,validJobRequest,safeText} from './utils.mjs';
const now=()=>Math.floor(Date.now()/1000);
const headers={ 'cache-control':'no-store','x-content-type-options':'nosniff','referrer-policy':'no-referrer' };
const base=e=>String(e.PUBLIC_BASE_URL||'').replace(/\/$/,'');
const resource=e=>base(e)+'/mcp';
const authServer=e=>base(e);
const d1=e=>e.DB;
const row=async(q,...args)=>q.bind(...args).first();
const run=async(q,...args)=>q.bind(...args).run();
const bodyLimit=async(req,size=32768)=>{const s=await req.text();if(s.length>size)throw Error('Request too large');return s;};
const fail=(msg,status=400)=>err(msg,status);
const authChallenge=e=>json({error:'unauthorized'},401,{'www-authenticate':`Bearer resource_metadata="${base(e)}/.well-known/oauth-protected-resource/mcp"`});

function oauthRedirectAllowed(uri,allowedCsv=''){
 if(!uri)return false;
 try{
  const u=new URL(uri);
  if(u.protocol!=='https:'||u.hash)return false;
  if(
   u.origin==='https://chatgpt.com' &&
   !u.search &&
   /^\/connector\/oauth\/[A-Za-z0-9_-]+$/.test(u.pathname)
  ) return true;
  return String(allowedCsv||'').split(',').map(x=>x.trim()).filter(Boolean).includes(uri);
 }catch{
  return false;
 }
}

async function bearer(req,e,scope='elokaby:read') {
 const v=req.headers.get('authorization')||'';
 if(!v.startsWith('Bearer '))return null;
 const hash=await sha256(v.slice(7));
 const t=await row(d1(e).prepare('SELECT client_id, scope, resource, expires FROM oauth_tokens WHERE hash=? AND token_type=? AND consumed=0'),hash,'access');
 return t && t.expires>now() && t.resource===resource(e) && t.scope.split(' ').includes(scope)?t:null;
}
async function internal(req,e){return Boolean(e.JOB_SHARED_SECRET)&&timingEqual(req.headers.get('authorization')||'',`Bearer ${e.JOB_SHARED_SECRET}`);}
async function createJob(e,kind,actor,input){
 const id=uuid(),t=now();
 await run(d1(e).prepare('INSERT INTO jobs(id,kind,actor,input_json,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?)'),id,kind,actor,JSON.stringify(input),'queued',t,t);
 return id;
}
async function dispatchJob(e,id){
 if(!e.GITHUB_DISPATCH_TOKEN||!e.GITHUB_OWNER||!e.GITHUB_REPO)throw Error('GitHub dispatch is not configured');
 const owner=encodeURIComponent(e.GITHUB_OWNER),repo=encodeURIComponent(e.GITHUB_REPO);
 const res=await fetch(`https://api.github.com/repos/${owner}/${repo}/dispatches`,{
 method:'POST',headers:{authorization:`Bearer ${e.GITHUB_DISPATCH_TOKEN}`,accept:'application/vnd.github+json','x-github-api-version':'2022-11-28','user-agent':'el-okaby-ai-gateway','content-type':'application/json'},
 body:JSON.stringify({event_type:'el_okaby_job',client_payload:{job_id:id}})
 });
 if(res.status!==204){await run(d1(e).prepare('UPDATE jobs SET status=?,error=?,updated_at=? WHERE id=?'), 'dispatch_error',`GitHub HTTP ${res.status}`,now(),id);throw Error(`GitHub dispatch returned HTTP ${res.status}`);}
 await run(d1(e).prepare('UPDATE jobs SET status=?,dispatched_at=?,updated_at=? WHERE id=?'),'queued',now(),now(),id);
}
async function scheduleJob(e,kind,actor,input){
 const id=await createJob(e,kind,actor,input);
 try{await dispatchJob(e,id);}catch(ex){return {job_id:id,status:'dispatch_error',error:String(ex.message)};}
 return {job_id:id,status:'queued',instruction:'Call get_job with this job_id later. This is a fresh Meta query job, not cached data.'};
}
async function outputJob(e,id,actor){
 const j=await row(d1(e).prepare('SELECT id,kind,actor,status,result_json,error,created_at,updated_at FROM jobs WHERE id=?'),id);
 if(!j || j.actor!==actor)return {error:'Job not found'};
 const out={job_id:id,status:j.status,created_at:j.created_at,updated_at:j.updated_at};
 if(j.status==='done') {try{out.result=JSON.parse(j.result_json||'null');}catch{out.error='Result unavailable';}}
 else if(j.result_json){try{out.progress=JSON.parse(j.result_json);}catch{}}
 if(j.error && j.status!=='done')out.error=j.error;
 const r=await row(d1(e).prepare('SELECT filename,mime FROM reports WHERE job_id=?'),id);
 if(r && j.status==='done'){
  const token=randomToken(32);await run(d1(e).prepare('INSERT INTO download_tokens(hash,job_id,expires) VALUES(?,?,?)'),await sha256(token),id,now()+900);
  out.report={filename:r.filename,mime:r.mime,url:base(e)+'/download/'+token,expires_in_seconds:900};
 }
 return out;
}

const tools=[
 {name:'discover_accounts',description:'Discover authorized Meta ad accounts through configured access tokens. No account data is invented; errors and token coverage are returned.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]},
 {name:'discover_fields',description:'Get the Meta query field/action catalog and constraints. The catalog is advisory; Meta validates requested fields at execution.',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]},
 {name:'query_meta',description:'Run a fresh Meta Marketing API Insights query. Choose arbitrary Meta fields, breakdowns, level, date range or date_preset, account_ids, action_type, row_limit. For large queries use async job polling. Do not treat generic results as a common metric across objectives.',inputSchema:{type:'object',properties:{account_ids:{type:'array',items:{type:'string'}},level:{type:'string',enum:['account','campaign','adset','ad']},fields:{type:'array',items:{type:'string'}},breakdowns:{type:'array',items:{type:'string'}},since:{type:'string'},until:{type:'string'},date_preset:{type:'string'},time_increment:{type:['integer','string']},action_type:{type:'string'},row_limit:{type:'integer',minimum:1,maximum:10000},filtering:{type:'array',items:{type:'object'}}},additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]},
 {name:'inspect_creatives',description:'Read authorized ad creative metadata and media references; does not claim to interpret full video content.',inputSchema:{type:'object',properties:{account_ids:{type:'array',items:{type:'string'}},row_limit:{type:'integer',minimum:1,maximum:500}},additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]},
 {name:'analyze_data',description:'Run fresh Meta queries then deterministic numeric analysis, grouped by chosen fields and with an explicit action_type when calculating costs. Time comparisons and winners are possible without fixed reports.',inputSchema:{type:'object',properties:{query:{type:'object'},group_by:{type:'array',items:{type:'string'}},action_type:{type:'string'},min_spend:{type:'number'},sort_by:{type:'string'},top_n:{type:'integer',minimum:1,maximum:100}},required:['query'],additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]},
 {name:'export_report',description:'Produce a private CSV/XLSX/PDF from an on-demand query; returns a job id then a short-lived download URL from get_job.',inputSchema:{type:'object',properties:{query:{type:'object'},format:{type:'string',enum:['csv','xlsx','pdf']},title:{type:'string'}},required:['query','format'],additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]},
 {name:'start_historical_audit',description:'Start ONE resumable full historical Lead Generation audit across all accessible Meta ad accounts (or selected accounts), including account discovery, lifetime and daily performance, creative metadata, deterministic Winner/Potential/Underperformer classification, scan coverage/errors, and a private XLSX workbook. Use this tool for broad historical audit/report requests instead of decomposing the request into many query_meta/analyze_data calls. The logical audit checkpoints to private R2 and can continue across multiple GitHub runs.',inputSchema:{type:'object',properties:{account_ids:{type:'array',items:{type:'string'},maxItems:100},since:{type:'string'},until:{type:'string'},target_cpl:{type:'number',minimum:0},min_winner_leads:{type:'integer',minimum:3,maximum:10000},min_potential_leads:{type:'integer',minimum:1,maximum:10000},include_daily_consistency:{type:'boolean'}},additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]},
 {name:'get_job',description:'Poll an asynchronous Meta job and return results or a private report download link. Does not re-query cached metrics as current data.',inputSchema:{type:'object',properties:{job_id:{type:'string'}},required:['job_id'],additionalProperties:false},annotations:{readOnlyHint:true},securitySchemes:[{type:'oauth2',scopes:['elokaby:read']}]}
];
async function mcp(req,e){
 if(req.method==='GET')return fail('SSE is not supported; use Streamable HTTP POST',405);
 if(req.method!=='POST')return fail('Method not allowed',405);
 let p;try{p=JSON.parse(await bodyLimit(req));}catch{return fail('Invalid JSON');}
 if(Array.isArray(p))return fail('Batch requests not supported');
 const id=p?.id,method=p?.method;
 if(method==='notifications/initialized')return new Response(null,{status:202,headers});
 const answer=(result)=>json({jsonrpc:'2.0',id,result},200,{'mcp-protocol-version':'2025-06-18'});
 const rpcErr=(message,code=-32602)=>json({jsonrpc:'2.0',id,error:{code,message}});
 if(id===undefined)return rpcErr('Request id required',-32600);
 if(method==='initialize')return answer({protocolVersion:'2025-06-18',capabilities:{tools:{}},serverInfo:{name:'el-okaby-ai-analyst',version:'1.1.0'}});
 if(method==='ping')return answer({});
 if(method==='tools/list')return answer({tools});
 if(method!=='tools/call')return rpcErr('Method not found',-32601);

 const auth=await bearer(req,e);
 if(!auth){
  return answer({
   content:[{type:'text',text:'Authentication required: no valid access token provided.'}],
   _meta:{
    'mcp/www_authenticate':[
     `Bearer resource_metadata="${base(e)}/.well-known/oauth-protected-resource/mcp", error="insufficient_scope", error_description="Authentication required to use El Okaby AI Analyst"`
    ]
   },
   isError:true
  });
 }

 const name=p.params?.name,a=p.params?.arguments||{};
 const tool=tools.find(x=>x.name===name);if(!tool)return rpcErr('Unknown tool');
 try{
  if(name==='get_job'){
   if(!/^[0-9a-f-]{36}$/i.test(a.job_id||''))throw Error('Invalid job id');
   return answer({content:[{type:'text',text:JSON.stringify(await outputJob(e,a.job_id,'owner'))}]});
  }
  if(name==='discover_accounts'||name==='discover_fields') { if(Object.keys(a).length)throw Error('No arguments expected'); }
  const input=validJobRequest({mode:name,params:a});
  const out=await scheduleJob(e,'mcp','owner',input);
  return answer({content:[{type:'text',text:JSON.stringify(out)}]});
 }catch(ex){return answer({isError:true,content:[{type:'text',text:String(ex.message)}]});}
}

function metadata(e){return json({resource:resource(e),authorization_servers:[authServer(e)],scopes_supported:['elokaby:read'],bearer_methods_supported:['header']});}
function oauthMetadata(e){return json({
 issuer:authServer(e),
 authorization_endpoint:base(e)+'/authorize',
 token_endpoint:base(e)+'/token',
 response_types_supported:['code'],
 grant_types_supported:['authorization_code','refresh_token'],
 token_endpoint_auth_methods_supported:['none'],
 code_challenge_methods_supported:['S256'],
 scopes_supported:['elokaby:read'],
 client_id_metadata_document_supported:true
});}
async function register(req,e){
 if(req.method!=='POST')return fail('Method not allowed',405);
 const ip=await sha256(req.headers.get('cf-connecting-ip')||'unknown');
 const attempts=await row(d1(e).prepare('SELECT COUNT(*) AS n FROM registration_attempts WHERE ip_hash=? AND created_at>?'),ip,now()-3600);
 if((attempts?.n||0)>=10)return fail('Registration rate limit exceeded',429);
 let p;try{p=JSON.parse(await bodyLimit(req,8192));}catch{return fail('Invalid JSON');}
 if(!Array.isArray(p.redirect_uris)||!p.redirect_uris.length||p.redirect_uris.length>5||!p.redirect_uris.every(u=>oauthRedirectAllowed(u,e.OAUTH_ALLOWED_REDIRECT_URIS)))return fail('Redirect URI not allowed');
 if(p.token_endpoint_auth_method && p.token_endpoint_auth_method!=='none')return fail('Only public clients with PKCE supported');
 const id=randomToken(24),created=now();
 await run(d1(e).prepare('INSERT INTO registration_attempts(ip_hash,created_at) VALUES(?,?)'),ip,created);
 await run(d1(e).prepare('INSERT INTO oauth_clients(client_id,redirect_uris,created_at) VALUES(?,?,?)'),id,JSON.stringify(p.redirect_uris),created);
 return json({client_id:id,client_id_issued_at:created,redirect_uris:p.redirect_uris,grant_types:['authorization_code','refresh_token'],response_types:['code'],token_endpoint_auth_method:'none'},201);
}
async function resolveCimdClient(clientId,redirectUri){
 try{
  const u=new URL(clientId);

  // Accept only ChatGPT callback-specific CIMD identities.
  if(
   u.protocol!=='https:' ||
   u.hostname!=='chatgpt.com' ||
   u.username || u.password || u.search || u.hash
  ) return null;

  const m=u.pathname.match(/^\/oauth\/([A-Za-z0-9_-]+)\/client\.json$/);
  if(!m)return null;

  const callbackId=m[1];
  const expectedRedirect=`https://chatgpt.com/connector/oauth/${callbackId}`;
  if(redirectUri!==expectedRedirect)return null;

  // Prefer validating the live CIMD document. Some edge paths may reject or
  // redirect server-to-server requests, so use a strict ChatGPT URL-binding
  // fallback only for this private MCP.
  try{
   const res=await fetch(u.toString(),{
    method:'GET',
    headers:{
     accept:'application/json',
     'user-agent':'El-Okaby-AI-Analyst/1.0'
    },
    redirect:'follow'
   });

   if(res.ok){
    const raw=await res.text();
    if(raw.length<=32768){
     let meta=null;
     try{meta=JSON.parse(raw);}catch{}

     if(meta){
      const redirects=Array.isArray(meta.redirect_uris)?meta.redirect_uris:[];
      const responseTypes=Array.isArray(meta.response_types)?meta.response_types:[];
      const grantTypes=Array.isArray(meta.grant_types)?meta.grant_types:[];
      const methods=Array.isArray(meta.token_endpoint_auth_methods_supported)
       ? meta.token_endpoint_auth_methods_supported
       : (meta.token_endpoint_auth_method?[meta.token_endpoint_auth_method]:[]);

      if(
       meta.client_id===u.toString() &&
       redirects.includes(expectedRedirect) &&
       (!responseTypes.length || responseTypes.includes('code')) &&
       (!grantTypes.length || grantTypes.includes('authorization_code')) &&
       (!methods.length || methods.includes('none') || methods.includes('private_key_jwt'))
      ){
       return {client_id:u.toString(),metadata:meta,verified:'cimd'};
      }
     }
    }
   }
  }catch{}

  // Strict fallback: the client_id and redirect_uri must share the same
  // ChatGPT-issued callback id and both remain on chatgpt.com.
  return {
   client_id:u.toString(),
   metadata:null,
   verified:'strict_chatgpt_callback_binding'
  };
 }catch{
  return null;
 }
}

async function authorize(req,e){
 if(req.method==='GET'){
  const p=new URL(req.url).searchParams,client=p.get('client_id')||'',redirect=p.get('redirect_uri')||'',resourceParam=p.get('resource')||'',challenge=p.get('code_challenge')||'';
  const cimd=await resolveCimdClient(client,redirect);
  if(!cimd||!oauthRedirectAllowed(redirect,e.OAUTH_ALLOWED_REDIRECT_URIS)||resourceParam!==resource(e)||p.get('response_type')!=='code'||p.get('code_challenge_method')!=='S256'||!/^[a-zA-Z0-9_-]{43,128}$/.test(challenge)||!(p.get('scope')||'').split(' ').includes('elokaby:read'))return fail('Invalid OAuth authorization request');
  const nonce=randomToken(20),ip=await sha256(req.headers.get('cf-connecting-ip')||'unknown');
  await run(d1(e).prepare('INSERT INTO oauth_intents(id,client_id,redirect_uri,code_challenge,state,scope,resource,ip_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)'),nonce,client,redirect,challenge,p.get('state')||'','elokaby:read',resourceParam,ip,now());
  return new Response(`<!doctype html><html><head><meta name="viewport" content="width=device-width"><title>El Okaby authorization</title></head><body><main><h2>El Okaby AI Analyst</h2><p>Authorize read-only access to ad analytics for the connected client.</p><form method="post" action="/authorize"><input type="hidden" name="intent" value="${safeText(nonce)}"><label>Owner password <input name="password" type="password" autocomplete="current-password" required></label><button type="submit">Authorize</button></form></main></body></html>`,{status:200,headers:{...headers,'content-type':'text/html; charset=utf-8','content-security-policy':"default-src 'none'; style-src 'unsafe-inline'; form-action 'self' https://chatgpt.com; base-uri 'none'; frame-ancestors 'none'"}});
 }
 if(req.method!=='POST')return fail('Method not allowed',405);
 const form=new URLSearchParams(await bodyLimit(req,2000)),intent=form.get('intent')||'',password=form.get('password')||'';
 const ip=await sha256(req.headers.get('cf-connecting-ip')||'unknown');
 const failures=await row(d1(e).prepare('SELECT COUNT(*) AS n FROM login_failures WHERE ip_hash=? AND created_at>?'),ip,now()-900);
 if((failures?.n||0)>=5)return fail('Too many attempts; retry later',429);
 const o=await row(d1(e).prepare('SELECT * FROM oauth_intents WHERE id=?'),intent);
 if(!o||o.ip_hash!==ip||o.created_at<now()-300)return fail('Expired authorization request');
 if(!e.MCP_OWNER_PASSWORD||!timingEqual(password,e.MCP_OWNER_PASSWORD)){
  await run(d1(e).prepare('INSERT INTO login_failures(ip_hash,created_at) VALUES(?,?)'),ip,now());return fail('Invalid credentials',401);
 }
 const consumed=await run(d1(e).prepare('DELETE FROM oauth_intents WHERE id=? AND created_at>=?'),intent,now()-300);
 if(!consumed.meta?.changes)return fail('Authorization already used');
 const code=randomToken(32);
 await run(d1(e).prepare('INSERT INTO oauth_codes(hash,client_id,redirect_uri,code_challenge,scope,resource,expires) VALUES(?,?,?,?,?,?,?)'),await sha256(code),o.client_id,o.redirect_uri,o.code_challenge,o.scope,o.resource,now()+300);
 const dest=new URL(o.redirect_uri);dest.searchParams.set('code',code);dest.searchParams.set('state',o.state);
 return new Response(null,{status:303,headers:{...headers,location:dest.toString()}});
}
async function issue(e,clientId,scope,resourceId){
 const access=randomToken(32),refresh=randomToken(32),t=now();
 await d1(e).batch([
  d1(e).prepare('INSERT INTO oauth_tokens(hash,client_id,scope,resource,expires,token_type) VALUES(?,?,?,?,?,?)').bind(await sha256(access),clientId,scope,resourceId,t+3600,'access'),
  d1(e).prepare('INSERT INTO oauth_tokens(hash,client_id,scope,resource,expires,token_type) VALUES(?,?,?,?,?,?)').bind(await sha256(refresh),clientId,scope,resourceId,t+30*86400,'refresh')
 ]);
 return json({access_token:access,refresh_token:refresh,token_type:'Bearer',expires_in:3600,scope});
}
async function token(req,e){
 if(req.method!=='POST')return fail('Method not allowed',405);
 const f=new URLSearchParams(await bodyLimit(req,4000)),grant=f.get('grant_type'),client=f.get('client_id')||'';
 if(!client)return fail('Missing client_id');
 if(grant==='authorization_code'){
  const verifier=f.get('code_verifier')||'',code=f.get('code')||'';
  if(!/^[a-zA-Z0-9._~-]{43,128}$/.test(verifier))return fail('Invalid verifier');
  const hash=await sha256(code),r=await row(d1(e).prepare('SELECT * FROM oauth_codes WHERE hash=?'),hash);
  if(!r||r.client_id!==client||r.expires<=now()||r.used||r.redirect_uri!==f.get('redirect_uri')||r.resource!==resource(e)|| (f.get('resource')&&f.get('resource')!==r.resource)||await sha256(verifier)!==r.code_challenge)return fail('Invalid or expired code');
  const consumed=await run(d1(e).prepare('UPDATE oauth_codes SET used=1 WHERE hash=? AND used=0'),hash);
  if(!consumed.meta?.changes)return fail('Code already used');
  return issue(e,client,r.scope,r.resource);
 }
 if(grant==='refresh_token'){
  const hash=await sha256(f.get('refresh_token')||'');
  const r=await row(d1(e).prepare('SELECT * FROM oauth_tokens WHERE hash=? AND token_type=?'),hash,'refresh');
  if(!r||r.client_id!==client||r.expires<=now()||r.consumed||r.resource!==resource(e)||(f.get('resource')&&f.get('resource')!==r.resource))return fail('Invalid refresh token');
  const consumed=await run(d1(e).prepare('UPDATE oauth_tokens SET consumed=1 WHERE hash=? AND consumed=0'),hash);
  if(!consumed.meta?.changes)return fail('Refresh token already consumed');
  return issue(e,client,r.scope,r.resource);
 }
 return fail('Unsupported grant_type');
}
async function whatsapp(req,e){
 if(req.method==='GET'){
  const p=new URL(req.url).searchParams;
  if(p.get('hub.mode')==='subscribe'&&e.WHATSAPP_VERIFY_TOKEN&&timingEqual(p.get('hub.verify_token')||'',e.WHATSAPP_VERIFY_TOKEN))return new Response(p.get('hub.challenge')||'',{headers:{...headers,'content-type':'text/plain'}});
  return fail('Verification failed',403);
 }
 if(req.method!=='POST')return fail('Method not allowed',405);
 const raw=await req.arrayBuffer();if(raw.byteLength>32768)return fail('Payload too large',413);
 if(!await hmacVerify(e.META_APP_SECRET,raw,req.headers.get('x-hub-signature-256')))return fail('Invalid webhook signature',403);
 let p;try{p=JSON.parse(new TextDecoder().decode(raw));}catch{return fail('Invalid JSON');}
 if(p.object!=='whatsapp_business_account')return json({ok:true,ignored:true});
 let accepted=0;
 for(const ent of p.entry||[]){
  if(e.WHATSAPP_WABA_ID && String(ent.id)!==String(e.WHATSAPP_WABA_ID))continue;
  for(const c of ent.changes||[]){
   const v=c.value||{};
   if(c.field!=='messages'||(e.WHATSAPP_PHONE_NUMBER_ID&&String(v.metadata?.phone_number_id)!==String(e.WHATSAPP_PHONE_NUMBER_ID)))continue;
   for(const m of v.messages||[]){
    if(m.type!=='text'||!m.text?.body||!m.id||!m.from)continue;
    const allowed=(e.ALLOWED_WA_NUMBERS||'').split(',').map(x=>x.replace(/\D/g,'')).filter(Boolean);
    const sender=String(m.from).replace(/\D/g,'');if(!allowed.includes(sender))continue;
    if(m.text.body.length>3000)continue;
    const existing=await row(d1(e).prepare('SELECT w.job_id,j.status FROM wa_events w JOIN jobs j ON w.job_id=j.id WHERE w.message_id=?'),m.id);
    if(existing){
      if(existing.status==='dispatch_error'){try{await dispatchJob(e,existing.job_id);}catch{return fail('Dispatch retry failed',503);}}
      continue;
    }
    const id=await createJob(e,'whatsapp','wa:'+sender,{mode:'whatsapp_agent',params:{question:m.text.body,from:sender}});
    const ins=await run(d1(e).prepare('INSERT OR IGNORE INTO wa_events(message_id,job_id,created_at) VALUES(?,?,?)'),m.id,id,now());
    if(!ins.meta?.changes)continue;
    try{await dispatchJob(e,id);}catch{return fail('Dispatch unavailable, webhook will retry',503);}
    accepted++;
   }
  }
 }
 return json({ok:true,accepted});
}
async function internalEndpoint(req,e,path){
 if(!await internal(req,e))return fail('Unauthorized',401);
 const id=path[2];if(!id||!/^[0-9a-f-]{36}$/i.test(id))return fail('Invalid job id');
 const j=await row(d1(e).prepare('SELECT * FROM jobs WHERE id=?'),id);if(!j)return fail('Not found',404);
 if(req.method==='GET'&&path.length===3){
  if(j.status==='done')return fail('Already completed',409);
  let staleSeconds=1900;try{if(JSON.parse(j.input_json||'{}').mode==='start_historical_audit')staleSeconds=20000;}catch{}
  const claim=await run(d1(e).prepare('UPDATE jobs SET status=?,updated_at=? WHERE id=? AND (status IN (?,?) OR (status=? AND updated_at<?))'),'running',now(),id,'queued','dispatch_error','running',now()-staleSeconds);
  if(!claim.meta?.changes)return fail('Job is already running or completed',409);
  let context=[];
  if(j.kind==='whatsapp'){
   const prior=await d1(e).prepare('SELECT input_json,result_json FROM jobs WHERE actor=? AND kind=? AND status=? AND id!=? ORDER BY created_at DESC LIMIT 2').bind(j.actor,'whatsapp','done',id).all();
   context=(prior.results||[]).reverse().map(x=>{try{return {question:JSON.parse(x.input_json).params.question,answer:JSON.parse(x.result_json).reply};}catch{return null;}}).filter(Boolean);
  }
  return json({job_id:id,kind:j.kind,input:JSON.parse(j.input_json),context});
 }
 if(path[3]==='checkpoint'){
  if(!e.REPORTS)return fail('R2 binding missing',503);
  const key='checkpoints/'+id+'.sqlite3.gz';
  if(req.method==='GET'){
   const obj=await e.REPORTS.get(key);if(!obj)return fail('Checkpoint not found',404);
   return new Response(obj.body,{headers:{...headers,'content-type':'application/gzip'}});
  }
  if(req.method==='POST'){
   const length=Number(req.headers.get('content-length')||0);
   if(!length)return fail('Content-Length is required for bounded checkpoint upload',411);
   if(length>80_000_000)return fail('Checkpoint too large',413);
   if((req.headers.get('content-type')||'')!=='application/gzip')return fail('Checkpoint must be application/gzip',415);
   await e.REPORTS.put(key,req.body,{httpMetadata:{contentType:'application/gzip'}});
   return json({ok:true});
  }
  return fail('Method not allowed',405);
 }
 if(req.method==='POST'&&path[3]==='progress'){
  let p;try{p=JSON.parse(await bodyLimit(req,20000));}catch{return fail('Bad JSON');}
  const progress=JSON.stringify(p||{});if(progress.length>18000)return fail('Progress payload too large',413);
  await run(d1(e).prepare("UPDATE jobs SET result_json=?,updated_at=? WHERE id=? AND status IN ('running','queued','continuing')"),progress,now(),id);
  return json({ok:true});
 }
 if(req.method==='POST'&&path[3]==='continue'){
  const moved=await run(d1(e).prepare("UPDATE jobs SET status='continuing',updated_at=? WHERE id=? AND status='running'"),now(),id);
  if(!moved.meta?.changes)return fail('Job is not in a continuable state',409);
  try{await dispatchJob(e,id);return json({ok:true,status:'queued'});}catch(ex){return fail('Continuation dispatch failed',503);}
 }
 if(req.method==='POST'&&path[3]==='report'){
  if(!e.REPORTS)return fail('R2 binding missing',503);
  const mime=req.headers.get('content-type')||'application/octet-stream';
  const ext=({'text/csv':'csv','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':'xlsx','application/pdf':'pdf'})[mime];
  const length=Number(req.headers.get('content-length')||0);if(!ext||length>80_000_000)return fail('Invalid report format or size',413);
  // Stream binary bytes to R2: Cloudflare does not parse or analyze the report.
  if(!length)return fail('Content-Length is required for bounded upload',411);
  const key='reports/'+id+'.'+ext;
  let filename=`el_okaby_${id}.${ext}`;
  try{const input=JSON.parse(j.input_json||'{}');if(input.mode==='start_historical_audit'&&ext==='xlsx')filename='El_Okaby_Historical_LeadGen_Winners_Analysis.xlsx';}catch{}
  await e.REPORTS.put(key,req.body,{httpMetadata:{contentType:mime}});
  await run(d1(e).prepare('INSERT OR REPLACE INTO reports(job_id,object_key,mime,filename,created_at) VALUES(?,?,?,?,?)'),id,key,mime,filename,now());
  return json({ok:true});
 }
 if(req.method==='GET'&&path[3]==='link'){
  const r=await row(d1(e).prepare('SELECT filename FROM reports WHERE job_id=?'),id);
  if(!r)return fail('No report',404);
  const token=randomToken(32);await run(d1(e).prepare('INSERT INTO download_tokens(hash,job_id,expires) VALUES(?,?,?)'),await sha256(token),id,now()+900);
  return json({url:base(e)+'/download/'+token,expires_in_seconds:900});
 }
 if(req.method==='POST'&&['complete','fail'].includes(path[3])){
  let p;try{p=JSON.parse(await bodyLimit(req,190000));}catch{return fail('Bad JSON');}
  const ok=path[3]==='complete';
  const result=ok?JSON.stringify(p.result||{}):null;
  if(result && result.length>170000)return fail('Result too large: use report upload',413);
  await run(d1(e).prepare('UPDATE jobs SET status=?,result_json=?,error=?,updated_at=? WHERE id=? AND status!=?'),ok?'done':'failed',result,ok?null:String(p.error||'Job failed').slice(0,300),now(),id,'done');
  if(ok&&e.REPORTS){try{await e.REPORTS.delete('checkpoints/'+id+'.sqlite3.gz');}catch{}}
  return json({ok:true});
 }
 return fail('Not found',404);
}
async function claimAlert(req,e){
 if(!await internal(req,e))return fail('Unauthorized',401);
 let p;try{p=JSON.parse(await bodyLimit(req,3000));}catch{return fail('Invalid JSON');}
 if(!/^[a-f0-9]{64}$/.test(p.fingerprint||''))return fail('Invalid alert key');
 const t=now(),period=Math.max(3600,Math.min(604800,Number(p.cooldown_seconds)||86400));
 const q=await run(d1(e).prepare('INSERT INTO alerts(fingerprint,last_sent) VALUES(?,?) ON CONFLICT(fingerprint) DO UPDATE SET last_sent=excluded.last_sent WHERE alerts.last_sent<?'),p.fingerprint,t,t-period);
 return json({send:Boolean(q.meta?.changes)});
}
async function download(req,e,token){
 if(req.method!=='GET')return fail('Method not allowed',405);
 const h=await sha256(token||''),t=await row(d1(e).prepare('SELECT job_id,expires FROM download_tokens WHERE hash=?'),h);
 if(!t||t.expires<=now())return fail('Invalid or expired link',404);
 const r=await row(d1(e).prepare('SELECT * FROM reports WHERE job_id=?'),t.job_id);
 if(!r)return fail('Report unavailable',404);
 const obj=await e.REPORTS.get(r.object_key);if(!obj)return fail('Report unavailable',404);
 return new Response(obj.body,{headers:{...headers,'content-type':r.mime,'content-disposition':`attachment; filename="${r.filename}"`}});
}
export default {
 async fetch(req,e){
  const p=new URL(req.url).pathname,parts=p.split('/').filter(Boolean);
  try{
   if(p==='/health')return json({status:'ok',service:'el-okaby-ai-gateway'});
   if(p==='/.well-known/oauth-protected-resource'||p==='/.well-known/oauth-protected-resource/mcp')return metadata(e);
   if(p==='/.well-known/oauth-authorization-server')return oauthMetadata(e);
   if(p==='/register')return register(req,e);
   if(p==='/authorize')return authorize(req,e);
   if(p==='/token')return token(req,e);
   if(p==='/mcp')return mcp(req,e);
   if(p==='/webhook/whatsapp')return whatsapp(req,e);
   if(p==='/internal/alerts/claim')return claimAlert(req,e);
   if(parts[0]==='internal'&&parts[1]==='jobs')return internalEndpoint(req,e,parts);
   if(parts[0]==='download'&&parts[1])return download(req,e,parts[1]);
   return fail('Not found',404);
  }catch(ex){return json({error:'Request failed'},500);}
 },
 async scheduled(_evt,e){
  const cutoff=now()-30*86400;
  const expired=await d1(e).prepare('SELECT object_key FROM reports WHERE created_at<? LIMIT 20').bind(cutoff).all();
  for(const r of expired.results||[]){await e.REPORTS.delete(r.object_key);}
  for(const r of expired.results||[])await run(d1(e).prepare('DELETE FROM reports WHERE object_key=?'),r.object_key);
  await d1(e).batch([
   d1(e).prepare('DELETE FROM wa_events WHERE created_at<?').bind(cutoff),
   d1(e).prepare('DELETE FROM oauth_codes WHERE expires<?').bind(now()),
   d1(e).prepare('DELETE FROM oauth_intents WHERE created_at<?').bind(now()-3600),
   d1(e).prepare('DELETE FROM oauth_tokens WHERE expires<?').bind(now()),
   d1(e).prepare('DELETE FROM download_tokens WHERE expires<?').bind(now()),
   d1(e).prepare('DELETE FROM login_failures WHERE created_at<?').bind(now()-900),
   d1(e).prepare('DELETE FROM registration_attempts WHERE created_at<?').bind(now()-86400),
   d1(e).prepare('DELETE FROM jobs WHERE created_at<?').bind(cutoff)
  ]);
 }
};
