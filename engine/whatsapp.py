"""Direct WhatsApp Cloud API. WABA ID validates configuration; messages send by phone_number_id."""
import json
import os
import urllib.request
import urllib.error


def send_text(recipient,text):
 token=os.environ['WHATSAPP_ACCESS_TOKEN'];phone_id=os.environ['WHATSAPP_PHONE_NUMBER_ID']
 if not os.getenv('WHATSAPP_WABA_ID'):raise ValueError('WHATSAPP_WABA_ID is required')
 version=os.getenv('META_GRAPH_VERSION','v26.0')
 if not recipient.isdigit():raise ValueError('Invalid recipient')
 for start in range(0,len(text),3200):
  body=json.dumps({'messaging_product':'whatsapp','to':recipient,'type':'text','text':{'preview_url':False,'body':text[start:start+3200]}},ensure_ascii=False).encode('utf-8')
  req=urllib.request.Request(f'https://graph.facebook.com/{version}/{phone_id}/messages',data=body,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json','User-Agent':'ElOkabyAIAnalyst/1.0'},method='POST')
  try:
   with urllib.request.urlopen(req,timeout=35) as r:r.read()
  except urllib.error.HTTPError as ex:
   raise RuntimeError(f'WhatsApp HTTP {ex.code}; response body deliberately withheld') from None

def send_template(recipient,template_name,language,body_text):
 """Proactive messaging requires an approved Utility template with exactly 1 body text variable."""
 token=os.environ['WHATSAPP_ACCESS_TOKEN'];phone_id=os.environ['WHATSAPP_PHONE_NUMBER_ID']
 if not os.getenv('WHATSAPP_WABA_ID') or not recipient.isdigit() or not template_name:raise ValueError('WABA, recipient and approved template required')
 version=os.getenv('META_GRAPH_VERSION','v26.0')
 payload={
  'messaging_product':'whatsapp','to':recipient,'type':'template',
  'template':{'name':template_name,'language':{'code':language},'components':[{'type':'body','parameters':[{'type':'text','text':body_text[:900]}]}]}
 }
 req=urllib.request.Request(f'https://graph.facebook.com/{version}/{phone_id}/messages',data=json.dumps(payload,ensure_ascii=False).encode('utf-8'),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json','User-Agent':'ElOkabyAIAnalyst/1.0'},method='POST')
 try:
  with urllib.request.urlopen(req,timeout=35) as r:r.read()
 except urllib.error.HTTPError as ex:raise RuntimeError(f'WhatsApp Template send failed: HTTP {ex.code}') from None
