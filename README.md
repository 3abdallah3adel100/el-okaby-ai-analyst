# El Okaby AI Analyst — NEW STANDALONE PROJECT

**GitHub-first, Cloudflare-minimal pilot.** Nothing is imported from or written to an old El Okaby project. The only GitHub connection required is a **new repository that you create explicitly**. The package contains no actual tokens, account results or customer data.

## Actual architecture

```
ChatGPT MCP (OAuth) ─┐
                    ├─> Cloudflare Worker (auth / routing / D1 job ID)
Meta WhatsApp Webhook┘          │
                               ├─> repository_dispatch: job_id ONLY
                               │              │
                               │       GitHub Actions Python engine
                               │       ├─ Fresh Meta Marketing API calls
                               │       ├─ arbitrary approved fields/breakdowns/periods
                               │       ├─ deterministic aggregation and exports
                               │       └─ OpenAI agent for WhatsApp questions only
                               │              │
                               └<─ authenticated result / private R2 report
                                               │
                           ChatGPT polls get_job / WhatsApp Cloud API replies
```

GitHub handles **every actual Meta query and all analytics**; Cloudflare never calls Meta Insights, processes ad datasets, or calls OpenAI. Cloudflare handles small mandatory gateway work: webhook verification, OAuth, dispatch, job status, private R2 streaming and expiring download URLs. Periodic monitoring is optional, and it also runs in GitHub Actions. Every on-demand query fetches Meta data at *execution time*; Meta's attribution reporting can lag and GitHub queue delay is not zero.

**Critical policy warning:** GitHub's official Actions additional terms restrict using Actions as part of a serverless application and unrelated GitHub-hosted workloads. This design is a **test/pilot as explicitly requested**, not a guaranteed or endorsed production hosting architecture. See https://docs.github.com/en/site-policy/github-terms/github-terms-for-additional-products-and-features . Public-repository free minutes do not override those terms. A production deployment may need a compliant dedicated processing runtime. Never save report data into the public Git repository or public Actions artifacts/logs.

## Capabilities implemented

- New independent codebase; no dependencies on prior project.
- Arbitrary named Meta tokens (one JSON Secret), business IDs, account allowlist, deterministic token ownership, account discovery; token values never appear in responses.
- Dynamic read-only field/level/date/breakdown/filter Meta Insights API query, safe bounds, 28-day windowing, cursors, rate-limit backoff and usage headers. Important: no fixed WhatsApp/LeadGen reports; cost-per-result requires explicit Meta `action_type`; no untyped leads aggregation. Meta validates requested field combinations.
- Deterministic aggregation, explicit partial/failed-account status, ad creative **metadata** discovery (not actual image/video understanding), CSV/XLSX/PDF private exports.
- Read-only Remote MCP via Streamable HTTP; OAuth authorization code with S256 PKCE, dynamic client registration, owner login, short-lived access token + rotating refresh token. **Needs live security review and ChatGPT account validation.**
- Direct WhatsApp Cloud API webhook: signature verification, WABA + phone-number checks, sender allowlist, deduplication; WhatsApp GPT API agent calls same Meta tools. Direct replies are sent by the GitHub job.
- Optional hourly monitoring + daily WhatsApp summary; not enabled until owner configures it.
- Private D1 job/status and R2 report storage; expiring report links. No question text/phone numbers in GitHub dispatch events.

## Limitations / acceptance gates

- Not deployed: no real Meta token, GitHub repository, Cloudflare binding, WhatsApp API or ChatGPT OAuth connection was used to test live access.
- ChatGPT receives an asynchronous `job_id`; call `get_job` later. GitHub Actions may queue, be delayed, or be restricted by GitHub; a ChatGPT synchronous immediate full response is **not guaranteed**.
- This is on-demand API retrieval, not a complete lifetime warehouse. Per-job Meta call cap, 10,000-row cap, output size and report cap protect cost and correctness; `truncated`/`complete` are explicit. Large lifetime analysis can require smaller date/account batches and a future persistent warehouse.
- Report PDF is a compact English-technical summary of up to 350 rows; CSV/XLSX carry all rows actually retrieved. PDF Arabic text requires deliberate font shaping and QA.
- Creative fields yield names/metadata/thumbnail references; the system cannot honestly claim to have watched or understood a video without additional media download and vision tooling.
- OAuth is a custom pilot implementation, not independently audited. Worker Free CPU may be exceeded by crypto/auth on some requests; test Worker CPU before relying on it. D1/R2 free tiers and availability may also change.
- No automatic budget/spend hard-stop for OpenAI API; set project budget alerts and monitor usage. Proactive monitoring and daily summaries require a separately approved WhatsApp template with one body-text variable. The code blocks proactive free-form messages when this is missing.

**Detailed Arabic setup:** [`docs/START_HERE_AR.md`](docs/START_HERE_AR.md). **Verification checklist:** [`docs/VERIFY.md`](docs/VERIFY.md).
