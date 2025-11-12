package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"orchestrator/db"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// --- STRUCTS ---

type VideoMsg struct {
	ID         int64  `json:"id"`
	Path       string `json:"path"`
	StartFrame int64  `json:"start_frame"`
	Epoch      int64  `json:"epoch"`
}

type StopMsg struct {
	ID    int64  `json:"id"`
	Path  string `json:"path"`
	Key   string `json:"key"`
	Epoch int64  `json:"epoch"`
}

// --- WORKER STATUS (video_messages) ---

type WorkerStatus struct {
	Pool      *pgxpool.Pool
	BatchSize int
	IdleSleep time.Duration
	Handle    func(context.Context, []VideoMsg) error
}

func (w *WorkerStatus) Run(ctx context.Context) error {
	if w.BatchSize <= 0 {
		w.BatchSize = 100
	}
	if w.IdleSleep <= 0 {
		w.IdleSleep = 500 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rows, err := w.Pool.Query(ctx, `
            WITH cte AS (
              SELECT id
              FROM video_messages
              ORDER BY id
              FOR UPDATE SKIP LOCKED
              LIMIT $1
            )
            DELETE FROM video_messages m
            USING cte
            WHERE m.id = cte.id
            RETURNING m.id, m.path, m.start_frame, m.epoch
        `, w.BatchSize)
		if err != nil {
			return fmt.Errorf("pop video_messages: %w", err)
		}

		var batch []VideoMsg
		for rows.Next() {
			var m VideoMsg
			if err := rows.Scan(&m.ID, &m.Path, &m.StartFrame, &m.Epoch); err != nil {
				rows.Close()
				return fmt.Errorf("scan: %w", err)
			}
			batch = append(batch, m)
		}
		rows.Close()

		if len(batch) == 0 {
			time.Sleep(w.IdleSleep)
			continue
		}

		if err := w.Handle(ctx, batch); err != nil {
			return fmt.Errorf("handle batch: %w", err)
		}
	}
}

func WorkMessage(dbase *db.DataBase) error {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("workMessage ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	wrk := &WorkerStatus{
		Pool:      dbase.Pool,
		BatchSize: 100,
		Handle: func(ctx context.Context, msgs []VideoMsg) error {
			for _, m := range msgs {
				message, err := json.Marshal(m)
				if err != nil {
					return fmt.Errorf("workMessage ERR: record Marshaling ERR: %v", err)
				}

				record := &kgo.Record{
					Topic: "path",
					Key:   []byte(m.Path),
					Value: message,
				}

				if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
					return fmt.Errorf("workMessage ERR: producer sending message ERR: %v", err)
				}
			}
			return nil
		},
	}

	return wrk.Run(context.Background())
}

// --- WORKER STOP (stop_messages) ---

type WorkStatusStop struct {
	Pool      *pgxpool.Pool
	BatchSize int
	IdleSleep time.Duration
	Handle    func(context.Context, []StopMsg) error
}

func (w *WorkStatusStop) Run(ctx context.Context) error {
	if w.BatchSize <= 0 {
		w.BatchSize = 100
	}
	if w.IdleSleep <= 0 {
		w.IdleSleep = 500 * time.Millisecond
	}
	if w.Pool == nil {
		return fmt.Errorf("work(stop): Pool is nil")
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rows, err := w.Pool.Query(ctx, `
			WITH cte AS (
				SELECT id
				FROM stop_messages
				ORDER BY id
				FOR UPDATE SKIP LOCKED
				LIMIT $1
			)
			DELETE FROM stop_messages sm
			USING cte
			WHERE sm.id = cte.id
			RETURNING sm.id, sm.path, sm.key, sm.epoch
		`, w.BatchSize)
		if err != nil {
			return fmt.Errorf("work(stop): pop stop_messages: %w", err)
		}

		var batch []StopMsg
		for rows.Next() {
			var m StopMsg
			if err := rows.Scan(&m.ID, &m.Path, &m.Key, &m.Epoch); err != nil {
				rows.Close()
				return fmt.Errorf("work(stop): scan: %w", err)
			}
			batch = append(batch, m)
		}
		rows.Close()

		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.IdleSleep):
				continue
			}
		}

		if w.Handle != nil {
			if err := w.Handle(ctx, batch); err != nil {
				return fmt.Errorf("work(stop): handle batch: %w", err)
			}
		}
	}
}

func WorkStatus(dbase *db.DataBase) error {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("workStatus ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	wrk := &WorkStatusStop{
		Pool:      dbase.Pool,
		BatchSize: 100,
		Handle: func(ctx context.Context, msgs []StopMsg) error {
			for _, m := range msgs {
				message, err := json.Marshal(m)
				if err != nil {
					return fmt.Errorf("workStatus ERR: record Marshaling ERR: %v", err)
				}

				record := &kgo.Record{
					Topic: "stop",
					Key:   []byte(m.Path),
					Value: message,
				}

				if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
					return fmt.Errorf("workStatus ERR: producer sending message ERR: %v", err)
				}
			}
			return nil
		},
	}

	return wrk.Run(context.Background())
}
