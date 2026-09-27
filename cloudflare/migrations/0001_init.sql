CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, actor TEXT NOT NULL, input_json TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'queued', result_json TEXT, error TEXT,
 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, dispatched_at INTEGER
);
CREATE INDEX IF NOT EXISTS jobs_actor ON jobs(actor,created_at DESC);
CREATE TABLE IF NOT EXISTS wa_events (message_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS oauth_clients (client_id TEXT PRIMARY KEY, redirect_uris TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS oauth_intents (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL, redirect_uri TEXT NOT NULL,
 code_challenge TEXT NOT NULL, state TEXT NOT NULL, scope TEXT NOT NULL, resource TEXT NOT NULL,
 ip_hash TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS oauth_codes (
 hash TEXT PRIMARY KEY, client_id TEXT NOT NULL, redirect_uri TEXT NOT NULL,
 code_challenge TEXT NOT NULL, scope TEXT NOT NULL, resource TEXT NOT NULL,
 expires INTEGER NOT NULL, used INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS oauth_tokens (
 hash TEXT PRIMARY KEY, client_id TEXT NOT NULL, scope TEXT NOT NULL, resource TEXT NOT NULL,
 expires INTEGER NOT NULL, token_type TEXT NOT NULL, consumed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS login_failures (ip_hash TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS login_failures_ip ON login_failures(ip_hash,created_at);
CREATE TABLE IF NOT EXISTS reports (job_id TEXT PRIMARY KEY, object_key TEXT NOT NULL, mime TEXT NOT NULL, filename TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS download_tokens (hash TEXT PRIMARY KEY, job_id TEXT NOT NULL, expires INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS alerts (fingerprint TEXT PRIMARY KEY, last_sent INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS registration_attempts (ip_hash TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS registration_attempts_ip ON registration_attempts(ip_hash,created_at);
