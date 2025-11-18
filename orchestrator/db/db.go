package db

import (
	"context"
	"errors"
	"fmt"
	"orchestrator/statuses"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DataBase struct {
	Pool *pgxpool.Pool
}

const defaultURL = "postgres://postgres:pass@postgres:5432/postgres?sslmode=disable"
const initSQLPath = "./initdb/001_schema.sql"

var ErrVideoNotFound error = errors.New("video not found")
var ErrEpochNotEqual error = errors.New("ENE")

func NewDataBase(ctx context.Context) (*DataBase, error) {
	poolConfig, err := pgxpool.ParseConfig(defaultURL)
	if err != nil {
		return nil, fmt.Errorf("configuration ERR: %v", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("connection ERR: %v", err)
	}

	db := &DataBase{Pool: pool}

	if err := db.initSchema(ctx); err != nil {
		db.Pool.Close()
		return nil, fmt.Errorf("init schema ERR: %v", err)
	}

	return db, nil
}

func (dataBase *DataBase) Close() {
	dataBase.Pool.Close()
}

func (db *DataBase) initSchema(ctx context.Context) error {
	content, err := os.ReadFile(initSQLPath)
	if err != nil {
		return fmt.Errorf("cannot read initdb.sql: %w", err)
	}

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("cannot begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, string(content))
	if err != nil {
		return fmt.Errorf("schema execution ERR: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("schema commit ERR: %w", err)
	}

	return nil
}

func (db *DataBase) ChangeStatus(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("changeStatus ERR: starting transaction ERR: %v", err)
	}
	defer tx.Rollback(ctx)

	var status string
	var epoch int
	err = tx.QueryRow(ctx, `
        SELECT status, epoch FROM videos WHERE path = $1 FOR UPDATE
    `, path).Scan(&status, &epoch)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			fmt.Printf("path: %v\n", path)
			return ErrVideoNotFound
		}
		return fmt.Errorf("changeStatus ERR: selecting ERR: %v", err)
	}

	if status != statuses.Running {
		return nil
	}

	_, err = tx.Exec(ctx, `
        UPDATE videos SET status = $1 WHERE path = $2
    `, statuses.Stopped, path)
	if err != nil {
		return fmt.Errorf("changeStatus ERR: changing status ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO stop_messages(path, "key", epoch)
        VALUES ($1, $2, $3)
    `, path, fmt.Sprintf("stop_%d", epoch), epoch)
	if err != nil {
		return fmt.Errorf("changeStatus ERR: adding into stop_messages ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
        DELETE FROM current_processes WHERE path = $1
    `, path)
	if err != nil {
		return fmt.Errorf("changeStatus ERR: deleting from current_processes ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
        UPDATE videos SET epoch = epoch + 1 WHERE path = $1
    `, path)
	if err != nil {
		return fmt.Errorf("changeStatus ERR: increasing epoch ERR: %v", err)
	}

	return tx.Commit(ctx)
}

func (db *DataBase) CreateVideo(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("createVideo ERR: starting transaction ERR: %v", err)
	}
	defer tx.Rollback(ctx)

	var status string

	var startFrame int64
	var epoch int64
	var exists bool

	err = tx.QueryRow(ctx, `
		SELECT status, start_frame, epoch FROM videos WHERE path = $1 FOR UPDATE
	`, path).Scan(&status, &startFrame, &epoch)
	if err != nil {
		exists = false
	} else {
		exists = true
	}

	if exists && status != statuses.Stopped {
		return nil
	}

	if !exists {
		_, err = tx.Exec(ctx, `
			INSERT INTO videos(path, status, start_frame, epoch)
			VALUES ($1, $2, $3, $4)
		`, path, statuses.Waiting, -1, 0)
		if err != nil {
			return fmt.Errorf("createVideo ERR: creating scenario ERR: %v", err)
		}
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE videos SET status = $1 WHERE path = $2
		`, statuses.Waiting, path)
		if err != nil {
			return fmt.Errorf("createVideo ERR: changing status ERR: %v", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO video_messages(path, start_frame, epoch)
		VALUES ($1, $2, $3)
	`, path, startFrame, epoch)
	if err != nil {
		return fmt.Errorf("createVideo ERR: adding into video_messages ERR: %v", err)
	}

	return tx.Commit(ctx)
}

func (db *DataBase) SetEndOfScenario(ctx context.Context, path string, epoch int64) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("setEndOfScenario ERR: starting transaction ERR: %v", err)
	}
	defer tx.Rollback(ctx)

	var dbStatus string
	var dbEpoch int64

	err = tx.QueryRow(ctx, `
		SELECT status, epoch
		FROM videos
		WHERE path = $1
		FOR UPDATE
	`, path).Scan(&dbStatus, &dbEpoch)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrVideoNotFound
		}
		return fmt.Errorf("setEndOfScenario ERR: reading video data ERR: %v", err)
	}

	if dbStatus != statuses.Running || dbEpoch != epoch {
		return nil
	}

	_, err = tx.Exec(ctx, `
		UPDATE videos
		SET status = $1
		WHERE path = $2
	`, statuses.Ended, path)
	if err != nil {
		return fmt.Errorf("setEndOfScenario ERR: changing status ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
		DELETE FROM current_processes
		WHERE path = $1
	`, path)
	if err != nil {
		return fmt.Errorf("setEndOfScenario ERR: deleting from current_processes ERR: %v", err)
	}

	return tx.Commit(ctx)
}

func (db *DataBase) SetStartOfScenario(ctx context.Context, path string, epoch int64) error {
	fmt.Printf("starting setting running. path: %v, epoch: %v\n", path, epoch)

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("setStartOfScenario ERR: starting transaction ERR: %v", err)
	}
	defer tx.Rollback(ctx)

	var status string
	var dbEpoch int64

	err = tx.QueryRow(ctx, `
		SELECT status, epoch 
		FROM videos 
		WHERE path = $1 
		FOR UPDATE
	`, path).Scan(&status, &dbEpoch)
	if err != nil {
		return fmt.Errorf("setStartOfScenario ERR: reading video data ERR: %v", err)
	}

	if status == statuses.Ended || epoch < dbEpoch {
		return ErrEpochNotEqual
	}

	_, err = tx.Exec(ctx, `
		UPDATE videos 
		SET status = $1 
		WHERE path = $2
	`, statuses.Running, path)
	if err != nil {
		return fmt.Errorf("setStartOfScenario ERR: changing status ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO current_processes(path, change_time)
		VALUES ($1, NOW())
		ON CONFLICT (path) DO NOTHING
	`, path)
	if err != nil {
		return fmt.Errorf("setStartOfScenario ERR: adding into current_processes ERR: %v", err)
	}

	return tx.Commit(ctx)
}

func (db *DataBase) AddObject(ctx context.Context, path, object string, startFrame, epoch int64) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("addObject ERR: starting transaction ERR: %v", err)
	}
	defer tx.Rollback(ctx)

	var status string
	var existingObject string
	var currentStartFrame int64
	var dbEpoch int64

	err = tx.QueryRow(ctx, `
		SELECT status, "object", start_frame, epoch 
		FROM videos 
		WHERE path = $1 
		FOR UPDATE
	`, path).Scan(&status, &existingObject, &currentStartFrame, &dbEpoch)
	if err != nil {
		return fmt.Errorf("addObject ERR: getting data ERR: %v", err)
	}

	if dbEpoch != epoch {
		return nil
	}

	if status != statuses.Running && status != statuses.Ended {
		return fmt.Errorf("addObject ERR: status is %v. Should be Running or Ended. path: %v, frame: %v, epoch %v", status, path, startFrame, epoch)
	}

	newObject := existingObject + " " + object
	_, err = tx.Exec(ctx, `
		UPDATE videos 
		SET "object" = $1 
		WHERE path = $2
	`, newObject, path)
	if err != nil {
		return fmt.Errorf("addObject ERR: adding object ERR: %v", err)
	}

	if startFrame > currentStartFrame {
		_, err = tx.Exec(ctx, `
			UPDATE videos 
			SET start_frame = $1 
			WHERE path = $2
		`, startFrame, path)
		if err != nil {
			return err
		}
	}

	_, err = tx.Exec(ctx, `
		UPDATE current_processes 
		SET change_time = NOW()
		WHERE path = $1
	`, path)
	if err != nil {
		return fmt.Errorf("addObject ERR: updating change_time ERR: %v", err)
	}

	return tx.Commit(ctx)
}

func (db *DataBase) Reset(ctx context.Context, path string) error {
	fmt.Printf("starting reseting path: %v\n", path)

	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("Reset ERR: starting transaction ERR: %v", err)
	}
	defer tx.Rollback(ctx)

	var status string
	var startFrame int64
	var epoch int64

	err = tx.QueryRow(ctx, `
		SELECT status, start_frame, epoch FROM videos WHERE path = $1 FOR UPDATE
	`, path).Scan(&status, &startFrame, &epoch)
	if err != nil {
		return fmt.Errorf("Reset ERR: getting data: %v", err)
	}

	if status != statuses.Running {
		return nil
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO stop_messages(path, "key", epoch)
        VALUES ($1, $2, $3)
    `, path, fmt.Sprintf("stop_%d", epoch), epoch)
	if err != nil {
		return fmt.Errorf("reset ERR: adding into stop_messages ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
        DELETE FROM current_processes WHERE path = $1
    `, path)
	if err != nil {
		return fmt.Errorf("reset ERR: deleting from current_processes ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
        UPDATE videos SET epoch = epoch + 1 WHERE path = $1
    `, path)
	if err != nil {
		return fmt.Errorf("reset ERR: increasing epoch ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
			UPDATE videos SET status = $1 WHERE path = $2
		`, statuses.Waiting, path)
	if err != nil {
		return fmt.Errorf("Reset ERR: changing status ERR: %v", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO video_messages(path, start_frame, epoch)
		VALUES ($1, $2, $3)
	`, path, startFrame, epoch+1)
	if err != nil {
		return fmt.Errorf("Reset ERR: adding into video_messages ERR: %v", err)
	}

	return tx.Commit(ctx)
}

func (db *DataBase) GetVideoMeta(ctx context.Context, path string) (status string, startFrame, epoch int64, err error) {
	err = db.Pool.QueryRow(ctx, `
		SELECT status, start_frame, epoch
		FROM videos
		WHERE path = $1
	`, path).Scan(&status, &startFrame, &epoch)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			fmt.Printf("path: %v\n", path)
			return "", -1, 0, ErrVideoNotFound
		}
		return "", -1, 0, fmt.Errorf("getVideoMeta ERR: %v", err)
	}

	return status, startFrame, epoch, nil
}

func (db *DataBase) GetVideoObject(ctx context.Context, path string) (object string, err error) {
	err = db.Pool.QueryRow(ctx, `
		SELECT "object"
		FROM videos
		WHERE path = $1
	`, path).Scan(&object)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			fmt.Printf("path: %v\n", path)
			return "", ErrVideoNotFound
		}
		return "", fmt.Errorf("getVideoObject ERR: %v", err)
	}

	return object, nil
}

func (db *DataBase) DeleteScenario(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("deleteScenario ERR: starting transaction ERR: %v", err)
	}
	defer tx.Rollback(ctx)

	var exists bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM videos WHERE path = $1)
	`, path).Scan(&exists)
	if err != nil {
		return fmt.Errorf("deleteScenario ERR: checking existence ERR: %v", err)
	}

	if !exists {
		return fmt.Errorf("deleteScenario ERR: video not found")
	}

	_, err = tx.Exec(ctx, `
		DELETE FROM videos WHERE path = $1
	`, path)
	if err != nil {
		return fmt.Errorf("deleteScenario ERR: deleting from videos ERR: %v", err)
	}

	return tx.Commit(ctx)
}

func (db *DataBase) GetCurrentProcesses(ctx context.Context) ([]string, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT path 
		FROM current_processes
	`)
	if err != nil {
		return nil, fmt.Errorf("getAllCurrentProcesses ERR: querying ERR: %v", err)
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("getAllCurrentProcesses ERR: scanning ERR: %v", err)
		}
		paths = append(paths, path)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getAllCurrentProcesses ERR: rows ERR: %v", err)
	}

	return paths, nil
}
