CREATE TABLE IF NOT EXISTS subscribers (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    spreadsheet_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS feeds (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE UNIQUE,
    name TEXT NOT NULL DEFAULT 'Default Feed',
    mode TEXT NOT NULL DEFAULT 'market',
    max_feeders INT NOT NULL DEFAULT 15,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (mode IN ('market', 'anchor')),
    CHECK (max_feeders BETWEEN 1 AND 15)
);

CREATE TABLE IF NOT EXISTS feeders (
    id BIGSERIAL PRIMARY KEY,
    feed_id BIGINT NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    handle TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'standard',
    niche_tag TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (feed_id, handle),
    CHECK (role IN ('anchor', 'standard'))
);

CREATE UNIQUE INDEX IF NOT EXISTS feeders_unique_anchor_per_feed
ON feeders (feed_id)
WHERE role = 'anchor' AND status = 'active';

CREATE TABLE IF NOT EXISTS feeder_pair_metrics (
    id BIGSERIAL PRIMARY KEY,
    feed_id BIGINT NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    anchor_feeder_id BIGINT NOT NULL REFERENCES feeders(id) ON DELETE CASCADE,
    feeder_id BIGINT NOT NULL REFERENCES feeders(id) ON DELETE CASCADE,
    window_days INT NOT NULL DEFAULT 7,
    velocity_delta NUMERIC,
    perf_delta NUMERIC,
    percentile_delta NUMERIC,
    relation_score NUMERIC,
    sample_size INT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (feed_id, anchor_feeder_id, feeder_id, window_days)
);

CREATE TABLE IF NOT EXISTS handle_state (
    handle TEXT NOT NULL,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    sheet_name TEXT NOT NULL,
    last_success_at TIMESTAMPTZ,
    last_seen_post_id TEXT,
    last_status TEXT,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subscriber_id, handle)
);

CREATE TABLE IF NOT EXISTS run_queue (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    spreadsheet_id TEXT NOT NULL,
    handle TEXT NOT NULL,
    run_type TEXT NOT NULL,
    attempt INT NOT NULL DEFAULT 0,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'pending',
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS run_queue_next_run_idx ON run_queue (next_run_at) WHERE status IN ('pending','retry');
CREATE INDEX IF NOT EXISTS run_queue_handle_idx ON run_queue (handle);

-- Prevent duplicate pending jobs per handle
CREATE UNIQUE INDEX IF NOT EXISTS run_queue_unique_pending
ON run_queue (subscriber_id, handle)
WHERE status IN ('pending','retry');

CREATE TABLE IF NOT EXISTS post_queue (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    spreadsheet_id TEXT NOT NULL,
    handle TEXT NOT NULL,
    post_url TEXT NOT NULL,
    checkpoint TEXT NOT NULL,
    requires_d7_hot BOOLEAN NOT NULL DEFAULT FALSE,
    attempt INT NOT NULL DEFAULT 0,
    next_run_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (checkpoint IN ('d3','d7','d21')),
    CHECK (status IN ('pending','running','retry','done','failed','skipped'))
);

CREATE INDEX IF NOT EXISTS post_queue_next_run_idx
ON post_queue (next_run_at)
WHERE status IN ('pending','retry');

CREATE UNIQUE INDEX IF NOT EXISTS post_queue_unique_checkpoint
ON post_queue (subscriber_id, handle, post_url, checkpoint);

CREATE TABLE IF NOT EXISTS run_log (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    spreadsheet_id TEXT NOT NULL,
    handle TEXT NOT NULL,
    run_type TEXT NOT NULL,
    status TEXT NOT NULL,
    apify_items_returned INT NOT NULL DEFAULT 0,
    posts_upserted_count INT NOT NULL DEFAULT 0,
    posts_updated_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS post_snapshots (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    handle TEXT NOT NULL,
    post_url TEXT NOT NULL,
    media_type TEXT,
    posted_at TIMESTAMPTZ,
    d0_at TIMESTAMPTZ,
    d1_at TIMESTAMPTZ,
    d2_at TIMESTAMPTZ,
    d3_at TIMESTAMPTZ,
    d7_at TIMESTAMPTZ,
    d21_at TIMESTAMPTZ,
    d0_views INT,
    d1_views INT,
    d2_views INT,
    d3_views INT,
    d7_views INT,
    d21_views INT,
    d0_likes INT,
    d1_likes INT,
    d2_likes INT,
    d3_likes INT,
    d7_likes INT,
    d21_likes INT,
    d0_comments INT,
    d1_comments INT,
    d2_comments INT,
    d3_comments INT,
    d7_comments INT,
    d21_comments INT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_id, handle, post_url)
);

CREATE TABLE IF NOT EXISTS post_signals (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    feed_id BIGINT REFERENCES feeds(id) ON DELETE SET NULL,
    feeder_id BIGINT REFERENCES feeders(id) ON DELETE SET NULL,
    handle TEXT NOT NULL,
    post_url TEXT NOT NULL,
    media_type TEXT,
    posted_at TIMESTAMPTZ,
    caption TEXT,
    velocity_tag TEXT,
    velocity_stage TEXT,
    velocity_percentile TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_id, handle, post_url)
);

CREATE INDEX IF NOT EXISTS post_signals_lookup_idx
ON post_signals (subscriber_id, velocity_tag, updated_at DESC);

CREATE TABLE IF NOT EXISTS post_embeddings (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    feed_id BIGINT REFERENCES feeds(id) ON DELETE SET NULL,
    feeder_id BIGINT REFERENCES feeders(id) ON DELETE SET NULL,
    niche_id TEXT,
    handle TEXT NOT NULL,
    post_url TEXT NOT NULL,
    signal_type TEXT NOT NULL DEFAULT 'caption_semantic',
    signal_version TEXT NOT NULL DEFAULT 'v1',
    embedding_model TEXT NOT NULL,
    embedding_dim INT NOT NULL,
    embedding_json JSONB NOT NULL,
    source_text TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_id, handle, post_url, embedding_model, signal_type)
);

CREATE INDEX IF NOT EXISTS post_embeddings_lookup_idx
ON post_embeddings (subscriber_id, signal_type, updated_at DESC);

CREATE TABLE IF NOT EXISTS handle_registry (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    handle TEXT NOT NULL,
    niche_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_id, handle)
);

CREATE INDEX IF NOT EXISTS handle_registry_subscriber_idx
ON handle_registry (subscriber_id, status);

CREATE TABLE IF NOT EXISTS handle_profile_metrics (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    handle TEXT NOT NULL,
    profile_url TEXT,
    full_name TEXT,
    business_category TEXT,
    biography TEXT,
    followers_count BIGINT,
    follows_count BIGINT,
    posts_count BIGINT,
    verified BOOLEAN,
    profile_pic_url TEXT,
    sampled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_id, handle)
);

CREATE INDEX IF NOT EXISTS handle_profile_metrics_subscriber_idx
ON handle_profile_metrics (subscriber_id, handle, sampled_at DESC);

CREATE TABLE IF NOT EXISTS posts_core (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    handle_id BIGINT REFERENCES handle_registry(id) ON DELETE SET NULL,
    handle TEXT NOT NULL,
    post_url TEXT NOT NULL,
    media_type TEXT,
    duration_seconds NUMERIC,
    posted_at TIMESTAMPTZ,
    caption TEXT,
    hashtags TEXT,
    caption_mentions TEXT,
    tagged_users TEXT,
    music_info TEXT,
    is_pinned BOOLEAN,
    paid_partnership BOOLEAN,
    sponsors TEXT,
    display_url TEXT,
    video_url TEXT,
    last_scanned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_id, handle, post_url)
);

CREATE INDEX IF NOT EXISTS posts_core_subscriber_handle_idx
ON posts_core (subscriber_id, handle, posted_at DESC);

CREATE TABLE IF NOT EXISTS post_checkpoint_metrics (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    feed_id BIGINT REFERENCES feeds(id) ON DELETE SET NULL,
    feeder_id BIGINT REFERENCES feeders(id) ON DELETE SET NULL,
    handle TEXT NOT NULL,
    post_url TEXT NOT NULL,
    checkpoint TEXT NOT NULL,
    checkpoint_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stage_label TEXT,
    views INT,
    likes INT,
    comments INT,
    metric_value NUMERIC,
    velocity_value NUMERIC,
    velocity_tag TEXT,
    velocity_percentile TEXT,
    perf_score TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (subscriber_id, handle, post_url, checkpoint)
);

CREATE INDEX IF NOT EXISTS post_checkpoint_metrics_lookup_idx
ON post_checkpoint_metrics (subscriber_id, handle, checkpoint, checkpoint_at DESC);

CREATE TABLE IF NOT EXISTS alert_events (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    handle TEXT NOT NULL,
    post_url TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    title TEXT,
    body TEXT,
    velocity_tag TEXT,
    velocity_stage TEXT,
    velocity_percentile TEXT,
    confidence NUMERIC,
    status TEXT NOT NULL DEFAULT 'new',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS alert_events_subscriber_status_idx
ON alert_events (subscriber_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS alert_candidates (
    id BIGSERIAL PRIMARY KEY,
    feed_id BIGINT NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    feeder_id BIGINT REFERENCES feeders(id) ON DELETE SET NULL,
    ui_tab TEXT NOT NULL DEFAULT 'flags',
    alert_category TEXT NOT NULL DEFAULT 'velocity',
    alert_color TEXT NOT NULL DEFAULT '#CCFF00',
    alert_urgency TEXT NOT NULL DEFAULT 'today',
    alert_dedupe_key TEXT NOT NULL DEFAULT '',
    alert_family TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    priority_score NUMERIC NOT NULL DEFAULT 0,
    impact_score NUMERIC NOT NULL DEFAULT 0,
    confidence_score NUMERIC NOT NULL DEFAULT 0,
    freshness_score NUMERIC NOT NULL DEFAULT 0,
    novelty_score NUMERIC NOT NULL DEFAULT 0,
    actionability_score NUMERIC NOT NULL DEFAULT 0,
    title TEXT,
    body TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    signal_window_start TIMESTAMPTZ,
    signal_window_end TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'candidate',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    selected_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    CHECK (status IN ('candidate', 'selected', 'sent', 'dropped')),
    CHECK (ui_tab IN ('flags')),
    CHECK (alert_category IN ('velocity', 'competitive', 'intelligence')),
    CHECK (alert_urgency IN ('now', 'today', 'watch'))
);

CREATE INDEX IF NOT EXISTS alert_candidates_feed_status_idx
ON alert_candidates (feed_id, status, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS alert_candidates_dedupe_idx
ON alert_candidates (feed_id, alert_dedupe_key)
WHERE alert_dedupe_key <> '';

CREATE TABLE IF NOT EXISTS alert_engine_state (
    feed_id BIGINT PRIMARY KEY REFERENCES feeds(id) ON DELETE CASCADE,
    last_hot_scan_at TIMESTAMPTZ,
    last_pattern_scan_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_aggregates (
    id BIGSERIAL PRIMARY KEY,
    feed_id BIGINT NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
    signal_type TEXT NOT NULL,
    signal_key TEXT NOT NULL,
    window_key TEXT NOT NULL,
    adoption_rate NUMERIC NOT NULL DEFAULT 0,
    velocity_delta NUMERIC NOT NULL DEFAULT 0,
    saturation_score NUMERIC NOT NULL DEFAULT 0,
    confidence NUMERIC NOT NULL DEFAULT 0,
    sample_size INT NOT NULL DEFAULT 0,
    source_start_at TIMESTAMPTZ,
    source_end_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (feed_id, signal_type, signal_key, window_key),
    CHECK (window_key IN ('d1', 'd2', 'd3', 'd7', 'd21'))
);

CREATE INDEX IF NOT EXISTS signal_aggregates_feed_window_idx
ON signal_aggregates (feed_id, window_key, updated_at DESC);

CREATE TABLE IF NOT EXISTS embedding_jobs (
    id BIGSERIAL PRIMARY KEY,
    subscriber_id BIGINT NOT NULL REFERENCES subscribers(id) ON DELETE CASCADE,
    handle TEXT,
    post_url TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS embedding_jobs_status_idx
ON embedding_jobs (subscriber_id, status, created_at);
