CREATE TABLE users (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0));
CREATE TABLE ledger (
 id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), delta BIGINT NOT NULL CHECK (delta <> 0),
 kind TEXT NOT NULL CHECK (kind IN ('payment','generation_debit','generation_refund')), reference TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(kind, reference)
);
CREATE INDEX ledger_user_created ON ledger(user_id, created_at DESC, id DESC);
CREATE TABLE models (slug TEXT PRIMARY KEY, price BIGINT NOT NULL CHECK(price > 0));
CREATE TABLE model_legs (
 model_slug TEXT NOT NULL REFERENCES models(slug), priority INT NOT NULL, provider TEXT NOT NULL,
 max_ref_images INT NOT NULL CHECK(max_ref_images >= 0), skip_on_moderation BOOLEAN NOT NULL DEFAULT false,
 PRIMARY KEY(model_slug, priority)
);
INSERT INTO models VALUES ('image-fast',2),('video-long',25);
INSERT INTO model_legs VALUES ('image-fast',0,'alpha',4,false),('image-fast',1,'beta',10,false),('video-long',0,'beta',2,false),('video-long',1,'alpha',1,true);
CREATE TABLE tasks (
 id UUID PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), model_slug TEXT NOT NULL REFERENCES models(slug),
 prompt TEXT NOT NULL, ref_images JSONB NOT NULL, status TEXT NOT NULL CHECK(status IN ('processing','completed','failed')),
 current_leg INT NOT NULL DEFAULT 0, last_error TEXT, result_url TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX tasks_active_user ON tasks(user_id) WHERE status='processing';
CREATE TABLE attempts (
 id BIGSERIAL PRIMARY KEY, task_id UUID NOT NULL REFERENCES tasks(id), leg_priority INT NOT NULL,
 provider TEXT NOT NULL, external_id TEXT, status TEXT NOT NULL CHECK(status IN ('sending','waiting','failed','completed','timed_out','skipped')),
 error_type TEXT, started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX attempts_one_active_per_task ON attempts(task_id) WHERE status IN ('sending','waiting');
CREATE INDEX attempts_waiting ON attempts(started_at) WHERE status='waiting';
CREATE TABLE processed_webhooks (provider TEXT NOT NULL, external_id TEXT NOT NULL, outcome TEXT NOT NULL, received_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(provider, external_id));
CREATE TABLE payments (payment_id TEXT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), tokens BIGINT NOT NULL CHECK(tokens > 0), status TEXT NOT NULL);
