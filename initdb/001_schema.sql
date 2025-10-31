CREATE TABLE IF NOT EXISTS videos (
    id          BIGSERIAL PRIMARY KEY,
    path        TEXT NOT NULL,
    object      TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL,
    start_frame INT  NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS video_messages (
    id          BIGSERIAL PRIMARY KEY,
    path        TEXT NOT NULL,
    start_frame INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stop_messages (
    id   BIGSERIAL PRIMARY KEY,
    path TEXT NOT NULL
);
