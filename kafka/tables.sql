CREATE TABLE raw_events (
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(10) NOT NULL,  -- 'like' или 'repost'
    user_id VARCHAR(21) NOT NULL,     -- Nano ID пользователя
    post_id VARCHAR(21) NOT NULL,     -- Nano ID поста
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE f_hourly_post_stats (
    hour_start TIMESTAMP NOT NULL,
    post_id VARCHAR(21) NOT NULL,
    likes_count INTEGER NOT NULL DEFAULT 0,
    reposts_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour_start, post_id)
);
-- Агрегированные факты по пользователям
CREATE TABLE f_hourly_user_stats (
    hour_start TIMESTAMP NOT NULL,
    user_id VARCHAR(21) NOT NULL,
    likes_given INTEGER NOT NULL DEFAULT 0,
    likes_received INTEGER NOT NULL DEFAULT 0,
    reposts_made INTEGER NOT NULL DEFAULT 0,
    reposts_received INTEGER NOT NULL DEFAULT 0,
    engagement_score INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hour_start, user_id)
);
CREATE TABLE dm_posts (
    post_id VARCHAR(21) PRIMARY NOT NULL,
    author_id VARCHAR(21) NOT NULL,
    created_at TIMESTAMP NOT NULL
);