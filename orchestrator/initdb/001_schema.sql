CREATE TABLE IF NOT EXISTS videos (
    id SERIAL PRIMARY KEY,
    "path" TEXT NOT NULL UNIQUE,
    "object" TEXT DEFAULT '',
    "status" TEXT NOT NULL,
    start_frame INT DEFAULT -1,
    epoch INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS video_messages (
    id SERIAL PRIMARY KEY,
    "path" TEXT NOT NULL UNIQUE,
    start_frame INT DEFAULT -1,
    epoch INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stop_messages (
    id SERIAL PRIMARY KEY,
    "path" TEXT NOT NULL UNIQUE,
    "key" TEXT NOT NULL,
    epoch INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS current_processes (
    id SERIAL PRIMARY KEY,
    "path" TEXT NOT NULL UNIQUE,
    start_frame INT DEFAULT -1
)