"""Private bounded exports. Never publish report artifacts on public GitHub."""
import csv
import io
import json
import re
from .analysis import summarize

def safe_cell(value):
 if isinstance(value,(dict,list)):value=json.dumps(value,ensure_ascii=False)
 if value is None:return ''
 s=str(value)
 if s.lstrip()[:1] in ('=','+','-','@') or s[:1] in ('\t','\r','\n'):return "'"+s
 return s[:32000]

def columns(rows):
 priority=['account_id','account_name','date_start','date_stop','campaign_id','campaign_name','adset_id','adset_name','ad_id','ad_name','spend','impressions','clicks','actions','cost_per_action_type']
 all_keys={k for r in rows for k in r.keys()}
 return [k for k in priority if k in all_keys]+sorted(all_keys-set(priority))

def make_report(result,fmt,title='El Okaby AI Analyst'):
 rows=result.get('rows',[]); cols=columns(rows)
 if fmt=='csv':
  b=io.StringIO(newline='');w=csv.writer(b);w.writerow(cols)
  for r in rows:w.writerow([safe_cell(r.get(k)) for k in cols])
  return b.getvalue().encode('utf-8-sig'),'text/csv'
 if fmt=='xlsx':
  import xlsxwriter
  b=io.BytesIO();wb=xlsxwriter.Workbook(b,{'in_memory':True});ws=wb.add_worksheet('Meta Insights')
  header=wb.add_format({'bold':True,'bg_color':'#12324A','font_color':'#FFFFFF','text_wrap':True});text_fmt=wb.add_format({'num_format':'@'}); money_fmt=wb.add_format({'num_format':'#,##0.00'})
  for j,k in enumerate(cols):ws.write_string(0,j,k,header);ws.set_column(j,j,19 if k.endswith('_id') else 24)
  for i,r in enumerate(rows,1):
   for j,k in enumerate(cols):
    v=r.get(k)
    if k in ('spend','impressions','clicks') and v not in (None,''):
     try:ws.write_number(i,j,float(v),money_fmt)
     except (ValueError,TypeError):ws.write_string(i,j,safe_cell(v),text_fmt)
    else:ws.write_string(i,j,safe_cell(v),text_fmt)
  ws.freeze_panes(1,0);ws.autofilter(0,0,max(1,len(rows)),max(0,len(cols)-1)) if cols else None
  notes=wb.add_worksheet('Read Me');notes.write(0,0,'Source: Meta API at job execution');notes.write(1,0,'Fresh at (UTC)');notes.write(1,1,result.get('live_at_utc',''))
  notes.write(2,0,'Complete');notes.write(2,1,str(result.get('complete')));notes.write(3,0,'Account errors');notes.write(3,1,json.dumps(result.get('account_errors',[]),ensure_ascii=False))
  notes.set_column(0,0,28);notes.set_column(1,1,90);wb.close();return b.getvalue(),'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
 if fmt=='pdf':
  from reportlab.lib import colors
  from reportlab.lib.enums import TA_LEFT
  from reportlab.lib.pagesizes import A4, landscape
  from reportlab.lib.styles import getSampleStyleSheet
  from reportlab.platypus import SimpleDocTemplate,Table,TableStyle,Paragraph,Spacer
  from xml.sax.saxutils import escape
  b=io.BytesIO();doc=SimpleDocTemplate(b,pagesize=landscape(A4),leftMargin=27,rightMargin=27,topMargin=25,bottomMargin=25)
  st=getSampleStyleSheet();story=[Paragraph(escape(re.sub(r'[^\x20-\x7E]','?',title[:100])),st['Title']),Paragraph('Meta source: '+str(result.get('live_at_utc',''))+' UTC; complete: '+str(result.get('complete')),st['Normal']),Spacer(1,12)]
  # For predictable printable PDF, select a compact numeric subset; full fidelity is in CSV/XLSX.
  small=[k for k in ['account_id','campaign_id','ad_id','date_start','spend','impressions','clicks'] if k in cols]
  if not small:small=cols[:6]
  cells=[[Paragraph(escape(k),st['BodyText']) for k in small]]
  for r in rows[:350]:cells.append([Paragraph(escape(safe_cell(r.get(k))[:38]).replace('\\n',' '),st['BodyText']) for k in small])
  if small:
   table=Table(cells,repeatRows=1,colWidths=[(landscape(A4)[0]-54)/len(small)]*len(small))
   table.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#12324A')),('TEXTCOLOR',(0,0),(-1,0),colors.white),('GRID',(0,0),(-1,-1),0.3,colors.HexColor('#CCCCCC')),('VALIGN',(0,0),(-1,-1),'TOP'),('FONTSIZE',(0,0),(-1,-1),7)]))
   story.append(table)
  if len(rows)>350:story.append(Paragraph('PDF preview limited to 350 rows; use XLSX for full result.',st['Normal']))
  doc.build(story);return b.getvalue(),'application/pdf'
 raise ValueError('format must be csv, xlsx, or pdf')
