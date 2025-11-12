package db

import (
	"context"
	"fmt"
	"orchestrator/statuses"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DataBase struct {
	Pool *pgxpool.Pool
}

const defaultURL = "postgres://postgres:password@postgres:5432/postgres?sslmode=disable"
const initSQLPath = "./initdb/001_schema.sql"

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

	// Инициализация таблиц
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

	// Выполняем SQL из файла
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
		return err
	}
	defer tx.Rollback(ctx)

	var status string
	var epoch int
	err = tx.QueryRow(ctx, `
        SELECT status, epoch FROM videos WHERE path = $1 FOR UPDATE
    `, path).Scan(&status, &epoch)
	if err != nil {
		// нет записи — ничего не делаем
		return nil
	}

	if status != statuses.Running {
		return nil
	}

	// 1) Обновляем статус
	_, err = tx.Exec(ctx, `
        UPDATE videos SET status = $1 WHERE path = $2
    `, statuses.Stopped, path)
	if err != nil {
		return err
	}

	// 2) Добавляем в stop_messages
	_, err = tx.Exec(ctx, `
        INSERT INTO stop_messages(path, "key", epoch)
        VALUES ($1, $2, $3)
    `, path, fmt.Sprintf("stop_%d", epoch), epoch)
	if err != nil {
		return err
	}

	// 3) Удаляем из current_processes
	_, err = tx.Exec(ctx, `
        DELETE FROM current_processes WHERE path = $1
    `, path)
	if err != nil {
		return err
	}

	// 4) Увеличиваем epoch
	_, err = tx.Exec(ctx, `
        UPDATE videos SET epoch = epoch + 1 WHERE path = $1
    `, path)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *DataBase) CreateVideo(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var status string
	var startFrame int
	var epoch int
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
		return nil // ничего не делаем
	}

	if !exists {
		// создаем новую запись
		_, err = tx.Exec(ctx, `
			INSERT INTO videos(path, status, start_frame, epoch)
			VALUES ($1, $2, $3, $4)
		`, path, statuses.Waiting, -1, 0)
		if err != nil {
			return err
		}
	} else {
		// 1) Меняем статус
		_, err = tx.Exec(ctx, `
			UPDATE videos SET status = $1 WHERE path = $2
		`, statuses.Waiting, path)
		if err != nil {
			return err
		}
	}

	// 2) Добавляем в video_messages
	_, err = tx.Exec(ctx, `
		INSERT INTO video_messages(path, start_frame, epoch)
		VALUES ($1, $2, $3)
	`, path, startFrame, epoch)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *DataBase) SetEndOfScenario(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// блокируем строку
	_, err = tx.Exec(ctx, `SELECT 1 FROM videos WHERE path = $1 FOR UPDATE`, path)
	if err != nil {
		return err
	}

	// 1) Меняем статус
	_, err = tx.Exec(ctx, `
		UPDATE videos SET status = $1 WHERE path = $2
	`, statuses.Ended, path)
	if err != nil {
		return err
	}

	// 2) Удаляем из current_processes
	_, err = tx.Exec(ctx, `
		DELETE FROM current_processes WHERE path = $1
	`, path)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *DataBase) SetStartOfScenario(ctx context.Context, path string) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// блокируем строку
	_, err = tx.Exec(ctx, `SELECT 1 FROM videos WHERE path = $1 FOR UPDATE`, path)
	if err != nil {
		return err
	}

	// 1) Меняем статус
	_, err = tx.Exec(ctx, `
		UPDATE videos SET status = $1 WHERE path = $2
	`, statuses.Running, path)
	if err != nil {
		return err
	}

	// 2) Добавляем в current_processes
	_, err = tx.Exec(ctx, `
		INSERT INTO current_processes(path, start_frame)
		VALUES ($1, -1)
		ON CONFLICT (path) DO NOTHING
	`, path)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *DataBase) AddObject(ctx context.Context, path, object string, startFrame int64) error {
	tx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var status string
	var existingObject string
	var currentStartFrame int64

	err = tx.QueryRow(ctx, `
		SELECT status, "object", start_frame FROM videos WHERE path = $1 FOR UPDATE
	`, path).Scan(&status, &existingObject, &currentStartFrame)
	if err != nil {
		return nil // нет записи — ничего не делаем
	}

	if status != statuses.Waiting && status != statuses.Running {
		return nil
	}

	if status == statuses.Waiting {
		// -1) Меняем статус
		_, err = tx.Exec(ctx, `
			UPDATE videos SET status = $1 WHERE path = $2
		`, statuses.Running, path)
		if err != nil {
			return err
		}

		// 0) Добавляем в current_processes
		_, err = tx.Exec(ctx, `
			INSERT INTO current_processes(path, start_frame)
			VALUES ($1, $2)
			ON CONFLICT (path) DO NOTHING
		`, path, startFrame)
		if err != nil {
			return err
		}
	}

	// 1) Добавляем объект к текущему
	newObject := existingObject + " " + object
	_, err = tx.Exec(ctx, `
		UPDATE videos SET "object" = $1 WHERE path = $2
	`, newObject, path)
	if err != nil {
		return err
	}

	// 2) Обновляем start_frame на максимум
	if startFrame > currentStartFrame {
		_, err = tx.Exec(ctx, `
			UPDATE videos SET start_frame = $1 WHERE path = $2
		`, startFrame, path)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
