# Verification status and acceptance gates (2026-09-28)

## Verified locally
- 12 Python tests for account allowlist, parameter validation, pagination, partial-account handling, metric separation and cross-account weighted cost, and sample XLSX/PDF export.
- 6 Node tests for webhook HMAC raw-byte check, strict OAuth redirect allowlist, job input bounds, health/OAuth discovery, unauthenticated MCP rejection.
- Python source compilation and Node syntax check.

## NOT verified live
- Meta App access tokens, WhatsApp WABA and phone_number_id, real Ads Insights fields/attribution for your accounts, and HM-003 access.
- GitHub repository_dispatch from Cloudflare, OAuth login with real ChatGPT Client callback/DCR/refresh, real ChatGPT Scan Tools.
- GitHub Actions runner environment/Dependency install, Cloudflare Wrangler deployment/free CPU, D1 migrations/R2 put/get, WhatsApp inbound+outbound delivery, template review, notification timing.
- GitHub Actions production policy eligibility; current hosted-runner architecture is PILOT only.

## Before treating as deployed
1. Create a new repository, keep old projects untouched, and set all Secrets/Variables per `START_HERE_AR.md`.
2. Install dependencies and run unit tests, deploy Worker, migrate D1, create private R2 bucket.
3. Inspect /health and OAuth discovery, then test a real OAuth authorization code + PKCE + refresh flow.
4. Scan Tools inside **your own** ChatGPT account and call `discover_accounts` via actual MCP.
5. Verify 1 ad account Today and same-day action counts directly against Ads Manager; test inaccessible account error and mapping.
6. Test raw signed WhatsApp webhook, one duplicate event, and a private outbound reply.
7. Test report generation and link expiry; inspect PDF output, and ensure no report or token appears in public logs.
8. Test Worker CPU usage with actual webhook and OAuth flows; handle 10ms Free limit explicitly.
9. Only then enable Monitoring and Daily Summary with approved template delivery and account-local timezone alignment.

Anything beyond the list of local tests is **implemented but unverified**, not described as connected or deployed.
