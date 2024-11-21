CREATE TABLE IF NOT EXISTS stg_shopify_apps (
    id SERIAL PRIMARY KEY,
    app_name VARCHAR(255),
    app_url VARCHAR(255),
    ranking INTEGER,
    average_rating DOUBLE PRECISION,
    total_reviews VARCHAR(255),
    is_ad BOOLEAN,
    sha256_surrogate_key VARCHAR(64),
    insert_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);