package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DataBase struct {
	Pool *pgxpool.Pool
}

const defaultURL = "postgres://postgres:ergotechnolain@postgres:5432/postgres?sslmode=disable"

func NewDataBase(ctx context.Context) (*DataBase, error) {
	poolConfig, err := pgxpool.ParseConfig(defaultURL)

	if err != nil {
		return nil, fmt.Errorf("configuration ERR: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)

	if err != nil {
		return nil, fmt.Errorf("connection ERR: %v", err)
	}

	dataBase := DataBase{
		Pool: pool,
	}

	return &dataBase, nil
}

func (dataBase *DataBase) Close() {
	dataBase.Pool.Close()
}

func (db *DataBase) CreateVideo(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var curStatus string
	var curStart int64

	err = tx.QueryRow(ctx, `
		SELECT status, start_frame
		FROM videos
		WHERE path = $1
		FOR UPDATE
	`, path).Scan(&curStatus, &curStart)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, err := tx.Exec(ctx, `
			INSERT INTO videos(path, status, start_frame)
			VALUES ($1, 'running', 0)
		`, path); err != nil {
			return fmt.Errorf("insert videos: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO video_messages(path, start_frame)
			VALUES ($1, 0)
		`, path); err != nil {
			return fmt.Errorf("insert video_messages: %w", err)
		}

	case err != nil:
		return fmt.Errorf("select videos: %w", err)

	default:
		if curStatus == "ended" || curStatus == "running" {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit no-op: %w", err)
			}
			return nil
		}

		if curStatus == "stop" {
			if _, err := tx.Exec(ctx, `
				UPDATE videos
				SET status = 'running'
				WHERE path = $1
			`, path); err != nil {
				return fmt.Errorf("update videos: %w", err)
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO video_messages(path, start_frame)
				VALUES ($1, $2)
			`, path, curStart); err != nil {
				return fmt.Errorf("insert video_messages after stop->running: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (db *DataBase) ChangeStatus(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	cmdTag, err := tx.Exec(ctx, `
		WITH updated AS (
		UPDATE videos
		SET status = 'stop'
		WHERE path = $1 AND status = 'running'
		RETURNING 1
		)
		INSERT INTO stop_message(path)
		SELECT $1
		FROM updated;
		`, path)
	if err != nil {
		return fmt.Errorf("exec: %v", err)
	}

	_ = cmdTag.RowsAffected()

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %v", err)
	}
	return nil
}

func (db *DataBase) SetEndOfScenario(ctx context.Context, path string) error {
	tag, err := db.Pool.Exec(ctx, `
		UPDATE videos
		SET status = 'ended'
		WHERE path = $1 AND status <> 'ended'
	`, path)
	if err != nil {
		return fmt.Errorf("update videos: %v", err)
	}

	_ = tag.RowsAffected()

	return nil
}

func (db *DataBase) AddObject(ctx context.Context, path, object string, startFrame int64) error {
	tag, err := db.Pool.Exec(ctx, `
		UPDATE videos
		SET "object" = CASE
				WHEN "object" = '' THEN $2
				ELSE "object" || ',' || $2
			END,
		    start_frame = $3
		WHERE "path" = $1
	`, path, object, startFrame)
	if err != nil {
		return fmt.Errorf("update videos: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("video not found for path=%q", path)
	}
	return nil
}

func (db *DataBase) DeleteStopMessage(ctx context.Context, path string) error {
	tag, err := db.Pool.Exec(ctx, `DELETE FROM stop_messages WHERE path = $1`, path)
	if err != nil {
		return fmt.Errorf("delete stop_messages: %w", err)
	}
	_ = tag.RowsAffected()
	return nil
}

func (db *DataBase) DeleteVideoMessage(ctx context.Context, path string) error {
	tag, err := db.Pool.Exec(ctx, `DELETE FROM video_messages WHERE path = $1`, path)
	if err != nil {
		return fmt.Errorf("delete video_messages: %w", err)
	}
	_ = tag.RowsAffected()
	return nil
}
