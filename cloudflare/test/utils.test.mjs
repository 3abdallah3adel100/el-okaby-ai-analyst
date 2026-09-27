import test from 'node:test';
import assert from 'node:assert/strict';
import {isAllowedRedirect, validJobRequest, sha256, hmacVerify, timingEqual} from '../src/utils.mjs';
test('redirect URI exact allowlist; no lookalikes',()=>{
 assert.equal(isAllowedRedirect('https://chatgpt.com/a','https://chatgpt.com/a'),true);
 assert.equal(isAllowedRedirect('https://chatgpt.com.evil/a','https://chatgpt.com/a'),false);
 assert.equal(isAllowedRedirect('http://chatgpt.com/a','https://chatgpt.com/a'),false);
});
test('only supported modes and bounded payload',()=>{
 assert.equal(validJobRequest({mode:'query_meta',params:{level:'ad'}}).mode,'query_meta');
 assert.throws(()=>validJobRequest({mode:'delete_ads'}));
 assert.throws(()=>validJobRequest({params:{huge:'x'.repeat(15000)}}));
});
test('digest and comparison', async()=>{
 assert.equal(await sha256('abc'),'ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0');
 assert.equal(timingEqual('same','same'),true);
 assert.equal(timingEqual('same','other'),false);
});
test('WhatsApp HMAC verifies raw body',async()=>{
 const key=await crypto.subtle.importKey('raw',new TextEncoder().encode('secret'),{name:'HMAC',hash:'SHA-256'},false,['sign']);
 const raw=new TextEncoder().encode('{"hello":1}');
 const sig=[...new Uint8Array(await crypto.subtle.sign('HMAC',key,raw))].map(x=>x.toString(16).padStart(2,'0')).join('');
 assert.equal(await hmacVerify('secret',raw,'sha256='+sig),true);
 assert.equal(await hmacVerify('secret',new TextEncoder().encode('{"hello":2}'),'sha256='+sig),false);
});
