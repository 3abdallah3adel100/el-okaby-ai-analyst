# دليل تنفيذ El Okaby AI Analyst — خطوة بخطوة

**المشروع ده جديد مستقل بالكامل**. متحطّوش داخل أي Repo قديم. كل السحب والتحليل وتشغيل OpenAI API للواتساب هيحصلوا على GitHub Actions. Cloudflare Gateway خفيف فقط: Webhook + OAuth + MCP + Dispatch + D1 + تمرير الملفات إلى R2. مفيش Meta Token في ChatGPT، ولا في GitHub Code، ولا في Cloudflare Worker إلا توكن الـGitHub Dispatch المطلوب للتوجيه. لا ترسل أي Access Token في الشات.

> ملاحظة أساسية قبل النشر: GitHub Actions المجاني في Public Repo مش ترخيص لاستخدامه كسيرفر إنتاجي لكل رسالة. شروط GitHub تحظر بعض استخداماته كجزء من Serverless Application. حزمة GitHub-first دي Pilot حسب اختيارك، وليست ضمان خدمة 24/7؛ الاستمرار يعتمد على سياسة GitHub والتجربة الفعلية. راجع https://docs.github.com/en/site-policy/github-terms/github-terms-for-additional-products-and-features . كذلك Free Workers حد CPU 10ms — لابد من قياس OAuth/Webhook فعليًا قبل التشغيل المستمر.

## المرحلة 1 — إنشاء Repository جديد

1. اعمل Repo **جديد** باسم `el-okaby-ai-analyst`، والأفضل Private بسبب الحساسية. لو اخترته Public، الكود والـWorkflow Logs ظاهرة للكل؛ ممنوع إضافة تقرير، رقم عميل، Token أو Raw Insights في الملفات/Actions logs. **ما تستخدمش أي Repo من المشاريع القديمة**.
2. ارفع **محتويات** مجلد المشروع (بما فيها `.github/workflows/`) إلى الفرع الافتراضي `main`. الـ`repository_dispatch` workflow لازم يكون على Default Branch.
3. خلي Actions مفعّلة. لا تفعل `ENABLE_MONITORING` أو `ENABLE_DAILY_SUMMARY` الآن.

## المرحلة 2 — Meta Marketing API

1. من Meta for Developers، اختار Meta App تابع للـBusiness المصرح له، وجهّز Access Tokens بصلاحيات قراءة الإعلانات المناسبة (`ads_read`، وصلاحيات Business discovery عند الحاجة). التوكن لازم يشوف الحساب نفسه؛ `Ads MCP Disabled` في اتصال آخر لا يحدد صلاحية مشروعنا.
2. أضف GitHub Actions **Repository Secrets**:

| Secret | القيمة | 
|---|---|
| `META_TOKENS_JSON` | JSON واحد بأسماء Token Aliases وقيم Access Tokens الحقيقية. مثال الشكل فقط: `{"primary":"TOKEN_VALUE","secondary":"TOKEN_VALUE"}` |
| `GATEWAY_BASE_URL` | رابط Worker بعد نشره: `https://NAME.workers.dev` |
| `JOB_SHARED_SECRET` | قيمة عشوائية قوية واحدة مطابقة لسر Cloudflare |
| `OPENAI_API_KEY` | OpenAI API Key للبوت على WhatsApp فقط |
| `WHATSAPP_ACCESS_TOKEN` | **توكن WhatsApp Cloud API المستقل**، وليس Ads Access Token |
| `ALERT_RECIPIENT` | رقمك الإداري الدولي بدون +؛ لا تفعل Alerts قبل الاختبار |

3. أضف GitHub Actions **Repository Variables**:

| Variable | مثال/الغرض |
|---|---|
| `META_ACCOUNT_ALLOWLIST` | `1234567890,2345678901` — **إجباري**؛ ضيف كل حساب مسموح |
| `META_ACCOUNT_TOKEN_MAP_JSON` | `{"1234567890":"primary","2345678901":"secondary"}` — يحدد Token المفضل للحساب |
| `META_BUSINESS_IDS` | `11111111111,22222222222` — أكثر من Business ID |
| `META_AGENT_MAP_JSON` | `{"1234567890":"Agent A","2345678901":"Agent B"}` — اختياري لتجميع الـAgents |
| `META_GRAPH_VERSION` | `v26.0` كتجربة أولية؛ تحقق من النسخة المعتمدة في App |
| `MAX_META_CALLS_PER_JOB` | `120` ميزانية حماية لكل Job |
| `OPENAI_MODEL` | موديل مدعوم في OpenAI API متاح لمشروعك، مثل `gpt-5-mini` |
| `WHATSAPP_WABA_ID` | WhatsApp Business Account ID |
| `WHATSAPP_PHONE_NUMBER_ID` | ID الرقم المرسل؛ مختلف عن WABA ID |
| `ENABLE_MONITORING` | `false` في البداية |
| `ENABLE_DAILY_SUMMARY` | `false` في البداية |
| `ALERT_TEMPLATE_NAME` | اسم WhatsApp Utility Template معتمد عند Meta يحتوي Body variable واحد للنص |
| `ALERT_TEMPLATE_LANGUAGE` | لغة الـTemplate المعتمد مثل `en_US` |
| `ALERT_ACTION_TYPE` | الحدث المحدد اللي هنقيسه، بعد التحقق من Meta actions في حساباتك |
| `ALERT_MIN_SPEND_EGP` | `500` مثال |
| `ALERT_CPL_MULTIPLIER` | `1.5` مثال |

**مهم:** الاختبار الأول يكون حساب واحد مصرح له، وبعدها نضيف باقي الحسابات والتوكنات. `HM-003` يُختبر بتوكنه الصحيح، ولو الحساب فشل نعرض Error ولا نحسبه صفر.

## المرحلة 3 — إنشاء Cloudflare Worker + D1 + R2

1. سجل في Cloudflare وافتح `cloudflare/` محليًا، وعندك Node.js مثبت.
2. نفّذ:

```bash
npm install
npx wrangler login
npx wrangler d1 create el-okaby-ai-jobs
npx wrangler r2 bucket create el-okaby-ai-private-reports
```

3. انسخ `database_id` الناتج إلى `cloudflare/wrangler.jsonc` بدل `REPLACE_WITH_D1_ID`.
4. عدّل `GITHUB_OWNER` و`GITHUB_REPO` لاسم **المستودع الجديد فقط**، و`PUBLIC_BASE_URL` إلى الرابط الحقيقي للـWorker. أضف `WHATSAPP_WABA_ID` و`WHATSAPP_PHONE_NUMBER_ID` إلى `vars` في الملف، و`ALLOWED_WA_NUMBERS` رقمك الإداري فقط (أكثر من رقم يفصلهم فاصلة، دولي بدون +).
5. هيظهر لك ChatGPT OAuth callback URL في شاشة Create MCP App؛ **قبل Scan Tools** انسخه حرفيًا إلى `OAUTH_ALLOWED_REDIRECT_URIS`. ممكن تستخدم أكثر من Callback مفصولين بفاصلة. ما تستخدمش Domain wildcard أو تسمح بأي redirect عشوائي.
6. نفذ:

```bash
npx wrangler d1 migrations apply el-okaby-ai-jobs --remote
```

7. ضيف Cloudflare Secrets في Terminal داخل `cloudflare/`، الأمر يطلب منك القيمة بدون كتابتها في الكود:

```bash
npx wrangler secret put GITHUB_DISPATCH_TOKEN
npx wrangler secret put JOB_SHARED_SECRET
npx wrangler secret put META_APP_SECRET
npx wrangler secret put WHATSAPP_VERIFY_TOKEN
npx wrangler secret put MCP_OWNER_PASSWORD
```

`GITHUB_DISPATCH_TOKEN`: Fine-grained PAT للمستودع الجديد فقط بصلاحية Repository Contents: Write اللازمة لـRepository Dispatch. `JOB_SHARED_SECRET`: قيمة عشوائية مطابقة للـGitHub Secret. `META_APP_SECRET`: من Meta App Settings. `WHATSAPP_VERIFY_TOKEN`: كلمة عشوائية تنشئها أنت لربط الـWebhook. `MCP_OWNER_PASSWORD`: كلمة مرور قوية لصفحة موافقة OAuth؛ دي حماية Pilot وليست بديلًا عن IdP مؤسسي/تقييم أمني.

8. شغّل `npx wrangler deploy`. اختبر `https://YOUR-WORKER.workers.dev/health` لازم يرجع `status:ok`.

**ملاحظة:** تفعيل R2 أو بعض خصائص Cloudflare قد يطلب إعداد Billing؛ لا نفترض إن كل حساب مؤهل للخطة المجانية بدون خطوة إضافية. تفقد Workers/D1/R2 limits من Dashboard.

## المرحلة 4 — اختبار GitHub Live Query بدون بيانات في Logs

- GitHub Workflow `on_demand.yml` يستقبل `repository_dispatch` من Worker بــ`job_id` **فقط**. السؤال، رقم واتساب والـResult محفوظين في D1 ومحميين بسر منفصل؛ لا تستخدم client_payload لنقل بيانات عملاء.
- Runner يعمل GET داخلي للسؤال، يستدعي Meta مباشرة في لحظة التشغيل، يدوّن نتيجة خاصة في D1 ويرفع ملف التقرير مباشرة إلى R2، لا إلى Public Artifacts.
- في Meta، يمكن أن يتأخر ظهور Attribution/Conversions حتى لو الاستعلام Fresh. كل Result فيه `live_at_utc` و`complete` و`truncated` و`account_errors`.
- `MAX_META_CALLS_PER_JOB` و`row_limit` سقف حماية: لو تجاوز الطلب الميزانية، يظهر إنه ناقص بدل ما يدعي اكتمال تاريخ الحساب.

**لا تنفّذ Live Tests بتوكن حقيقي قبل ضبط الـAllowlist وربط الـSecrets.** اتأكد إن GitHub Action يبدأ ويسحب بيانات حساب مصرح له، وراجع نفس الفترة والـAttribution في Ads Manager.

## المرحلة 5 — ربط ChatGPT MCP

1. افتح Create MCP App في حسابك.
2. ادخل رابط `https://YOUR-WORKER.workers.dev/mcp`؛ مش رابط `/health` أو GitHub Repo.
3. اختر OAuth إذا ظهر الاختيار. السيرفر ينشر `/.well-known/oauth-protected-resource/mcp` و`/.well-known/oauth-authorization-server` ويدعم DCR + Authorization Code + PKCE S256.
4. لازم يكون الـCallback الفعلي محفوظ بالضبط في `OAUTH_ALLOWED_REDIRECT_URIS` قبل Scan Tools. صفحة Authorization تسألك `MCP_OWNER_PASSWORD` وتوافق على وصول Read-only.
5. اختبر `Scan Tools` والأدوات: `discover_accounts`, `discover_fields`, `query_meta`, `analyze_data`, `inspect_creatives`, `export_report`, `get_job`.
6. سؤال مثال: «اسحب spend وactions لحساب واحد today». الأداة سترجع `job_id`; بعد تنفيذ GitHub، استعمل `get_job(job_id)` للنتيجة. **الانتظار طبيعي وليس Cached Report**.

إذا حصل OAuth/Scan Tools error، الحل من Log/رسالة الخطأ الفعلية وليس التخمين. إنشاء رابط MCP لا يثبت نجاح الاتصال بحسابك.

## المرحلة 6 — WhatsApp Cloud API مباشر (مش YCloud)

1. في Meta Developers جهّز WhatsApp Product ورقم إداري مربوط بالـWABA؛ اعرف الثلاث قيم المختلفة: `WHATSAPP_ACCESS_TOKEN` للإرسال من GitHub، `WHATSAPP_WABA_ID` للتحقق من الحساب الوارد، `WHATSAPP_PHONE_NUMBER_ID` للإرسال والتحقق من الرقم.
2. في Meta App Webhooks اختار Callback URL: `https://YOUR-WORKER.workers.dev/webhook/whatsapp`، وحط نفس `WHATSAPP_VERIFY_TOKEN` اللي في Cloudflare Secret. اشترك في حدث `messages` المناسب.
3. Cloudflare يتحقق من `X-Hub-Signature-256` باستخدام Meta App Secret، ومن WABA وPhone Number ومن ALLOWED_WA_NUMBERS، ويسجل Message ID لمنع التكرار.
4. لو الرسالة من رقمك المصرح: ينشئ `job_id` فقط ويبعته GitHub. GitHub يفتح سؤال المستخدم عبر Secret، ويستخدم OpenAI API لفهمه، ويستدعي Meta Live، وبعدين يرد بالـWhatsApp Cloud API. **Cloudflare لا يستخدم OPENAI_API_KEY ولا META_TOKENS_JSON.**
5. اسأل أولًا «إيه الحسابات المتاحة؟»؛ وبعدها «هات spend وactions today». اختبر متابعة «قارنه بامبارح». عند طلب تقرير يرسل رابطًا خاصًا مدته 15 دقيقة.

الرد على رسالة عميل وارد داخل نافذة الخدمة يختلف عن الإرسال الاستباقي. Alerts/Daily Summary خارج النافذة قد تحتاج WhatsApp Template معتمد؛ الكود يشترط Utility Template معتمدًا بمتغير Body واحد قبل أي إرسال استباقي؛ لازم يتوافق محتواه مع السياسة وتصنيفه وقت الموافقة. الإرسال التجريبي إلى رقمك فقط أولًا.

## المرحلة 7 — Monitoring / Daily Summary

بعد تأكيد Event صحيح محدد (`ALERT_ACTION_TYPE`)، فعّل `ENABLE_MONITORING=true` ثم شغّل Workflow يدويًا مرة. كوده يقارن Today بآخر 7 أيام، ويحتاج إنفاق حد أدنى وحجم Results سابقًا، ويستخدم `alerts` في D1 لمنع تكرار الإشعار. `ENABLE_DAILY_SUMMARY=true` يفعّل Workflow يوميًا؛ حساب الأرقام يتم بالكود ويظل مصدره Meta جديدًا في وقت التنفيذ. اعتبرها إشارات تشغيلية، مش تشخيصًا مؤكدًا.

**تحذير دقة:** الـMonitor الحالي يعتمد على تاريخ UTC عند تشغيل GitHub؛ لبعض الـAd Accounts ذات Timezone مختلف قد يحتاج تعديل Date Window بحسب account.timezone_name قبل الاعتماد الإنتاجي. لا تفعل Alerts في الإنتاج قبل الاختبار. إشعارات WhatsApp الاستباقية تحتاج Template حسب حالة نافذة المحادثة.

## المرحلة 8 — اختبار التكلفة والأمان قبل الاعتماد

- Cloudflare Free CPU الحقيقي من Analytics، لا من عدد الأسئلة وحده. الـGateway لا يحلل بيانات Meta، لكن OAuth/HMAC/D1 له CPU.
- OpenAI API منفصل عن اشتراك ChatGPT؛ لا يُستخدم في محادثة ChatGPT MCP إلا لو طلبت تشغيل AI من خارج ChatGPT. اضبط `OPENAI_MODEL` وميزانية مشروعك وراجع Usage؛ سعر السؤال يتوقف على الموديل والـTokens.
- Meta Usage Headers موجودة داخل نتائج Jobs. اختبر 1 ثم 3 ثم كل الحسابات، ولا تطلق 100 Job متزامنًا.
- GitHub Public Actions logs متاحة للعامة؛ لا تُدخل أسرارًا أو أسئلة أو بيانات عملاء إلى Actions input/echo. نحن نرسل Job ID فقط؛ راجع أي تعديل يدوي قبل الرفع.
- تأكد من صلاحيات التوكنات، فشل الحسابات، dedup، Token Rotation، ملف CSV بدون Formula Injection، انتهاء رابط التقرير، وتوافق Attribution مع Ads Manager.

## مصادر التنفيذ

- GitHub Actions policy: https://docs.github.com/en/site-policy/github-terms/github-terms-for-additional-products-and-features
- GitHub repository dispatch: https://docs.github.com/en/rest/repos/repos#create-a-repository-dispatch-event
- ChatGPT MCP OAuth requirements: https://developers.openai.com/plugins/build/auth
- Workers limits: https://developers.cloudflare.com/workers/platform/limits/
- D1/R2: https://developers.cloudflare.com/d1/ and https://developers.cloudflare.com/r2/
- Meta WhatsApp Cloud API: https://developers.facebook.com/docs/whatsapp/cloud-api/
