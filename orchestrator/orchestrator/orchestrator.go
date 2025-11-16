package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"orchestrator/db"

	"github.com/twmb/franz-go/pkg/kgo"
)

type object struct {
	ID     int64  `json:"id"`
	Path   string `json:"path"`
	Object string `json:"object"`
	Epoch  int64  `json:"epoch"`
}

type ending struct {
	EndPath string `json:"end_path"`
	Epoch   int64  `json:"epoch"`
}

func ChangeStatus(dataBase *db.DataBase) error {
	seeds := []string{
		"kafka_api_to_orchestrator1:9092",
		"kafka_api_to_orchestrator2:9092",
		"kafka_api_to_orchestrator3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("changeStatus"),
		kgo.ConsumeTopics("status"),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return fmt.Errorf("changeStatus ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(context.Background())

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("changeStatus ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if len(record.Value) == 0 {
				continue
			}

			var path string

			err = json.Unmarshal(record.Value, &path)

			if err != nil {
				return fmt.Errorf("changeStatus ERR: record Unmarshaling ERR: %v", err)
			}

			err = dataBase.ChangeStatus(context.Background(), path)

			if errors.Is(err, db.ErrVideoNotFound) {
				fmt.Println("changeStatus wrong path")
				continue
			}

			if err != nil {
				return fmt.Errorf("changeStatus ERR: db changing status ERR: %v", err)
			}

			err = cl.CommitUncommittedOffsets(context.Background())

			if err != nil {
				return fmt.Errorf("changeStatus ERR: offset commiting ERR: %v", err)
			}
		}
	}
}

func ProcessScenario(dataBase *db.DataBase) error {
	seeds := []string{
		"kafka_api_to_orchestrator1:9092",
		"kafka_api_to_orchestrator2:9092",
		"kafka_api_to_orchestrator3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("processScenario"),
		kgo.ConsumeTopics("path"),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return fmt.Errorf("processScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		ctx := context.Background()

		fetches := cl.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("processScenario ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				return fmt.Errorf("processScenario ERR: refusing to send empty value")
			}

			var path string

			err = json.Unmarshal(record.Value, &path)

			if err != nil {
				return fmt.Errorf("processScenario ERR: record Unmarshaling ERR: %v", err)
			}

			err = dataBase.CreateVideo(context.Background(), path)

			if err != nil {
				return fmt.Errorf("processScenario ERR: db creating video ERR: %v", err)
			}

			err = cl.CommitUncommittedOffsets(context.Background())

			if err != nil {
				return fmt.Errorf("processScenario ERR: offset commiting ERR: %v", err)
			}
		}
	}
}

func GetObject(dataBase *db.DataBase) error {
	seeds := []string{"kafka_inference_and_orchestrator1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("getObject"),
		kgo.ConsumeTopics("objects"),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return fmt.Errorf("getObject ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		ctx := context.Background()

		fetches := cl.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("getObject ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				return fmt.Errorf("getObject ERR: refusing to send empty value")
			}

			cl.CommitUncommittedOffsets(context.Background())

			var obj object

			err = json.Unmarshal(record.Value, &obj)

			if err != nil {
				return fmt.Errorf("getObject ERR: record Unmarshaling ERR: %v", err)
			}

			err = dataBase.AddObject(context.Background(), obj.Path, obj.Object, obj.ID, obj.Epoch)

			if err != nil {
				return fmt.Errorf("getObject ERR: %v", err)
			}
		}
	}
}

func SetEnd(dataBase *db.DataBase) error {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("setEnd"),
		kgo.ConsumeTopics("end"),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return fmt.Errorf("setEnd ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		ctx := context.Background()

		fetches := cl.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("setEnd ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				return fmt.Errorf("setEnd ERR: refusing to send empty value")
			}

			end := ending{}

			err = json.Unmarshal(record.Value, &end)

			if err != nil {
				return fmt.Errorf("setEnd ERR: record Unmarshaling ERR: %v", err)
			}

			err = dataBase.SetEndOfScenario(context.Background(), end.EndPath, end.Epoch)

			if err != nil {
				return fmt.Errorf("setEnd ERR: setting end ERR: %v", err)
			}

			err = cl.CommitUncommittedOffsets(context.Background())

			if err != nil {
				return fmt.Errorf("setEnd ERR: offset commiting ERR: %v", err)
			}
		}
	}
}

func DeleteScenario(dataBase *db.DataBase) error {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("deleteScenario"),
		kgo.ConsumeTopics("delete"),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return fmt.Errorf("deleteScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		ctx := context.Background()

		fetches := cl.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("deleteScenario ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				return fmt.Errorf("deleteScenario ERR: refusing to send empty value")
			}

			var path string

			err = json.Unmarshal(record.Value, &path)

			if err != nil {
				return fmt.Errorf("deleteScenario ERR: record Unmarshaling ERR: %v", err)
			}

			err = dataBase.DeleteScenario(context.Background(), path)

			if err != nil {
				return fmt.Errorf("deleteScenario ERR: setting end ERR: %v", err)
			}

			err = cl.CommitUncommittedOffsets(context.Background())

			if err != nil {
				return fmt.Errorf("deleteScenario ERR: offset commiting ERR: %v", err)
			}
		}
	}
}
