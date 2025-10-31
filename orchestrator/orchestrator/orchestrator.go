package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"orchestrator/db"

	"github.com/twmb/franz-go/pkg/kgo"
)

type object struct {
	ID     int64  `json:"id"`
	Path   string `json:"path"`
	Object string `json:"object"`
}

func ChangeStatus(db *db.DataBase) error {
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
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return fmt.Errorf("changeStatus ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		ctx := context.Background()

		fetches := cl.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("changeStatus ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()
		record := iter.Next()

		if len(record.Value) == 0 {
			continue
		}

		var path string

		err = json.Unmarshal(record.Value, &path)

		if err != nil {
			return fmt.Errorf("changeStatus ERR: record Unmarshaling ERR: %v", err)
		}

		err = db.ChangeStatus(context.Background(), path)

		if err != nil {
			return fmt.Errorf("changeStatus ERR: db changing status ERR: %v", err)
		}

		err = cl.CommitUncommittedOffsets(context.Background())

		if err != nil {
			return fmt.Errorf("changeStatus ERR: offset commiting ERR: %v", err)
		}
	}
}

func ProcessScenario(db *db.DataBase) error {
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
		kgo.AllowAutoTopicCreation(),
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
		record := iter.Next()

		if len(record.Value) == 0 {
			continue
		}

		var path string

		err = json.Unmarshal(record.Value, &path)

		if err != nil {
			return fmt.Errorf("processScenario ERR: record Unmarshaling ERR: %v", err)
		}

		err = db.CreateVideo(context.Background(), path)

		if err != nil {
			return fmt.Errorf("processScenario ERR: db creating video ERR: %v", err)
		}

		err = cl.CommitUncommittedOffsets(context.Background())

		if err != nil {
			return fmt.Errorf("processScenario ERR: offset commiting ERR: %v", err)
		}
	}
}

func GetObject(db *db.DataBase) error {
	seeds := []string{"kafka_inference_to_orchestrator1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("getObject"),
		kgo.ConsumeTopics("objects"),
		kgo.AllowAutoTopicCreation(),
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
		record := iter.Next()

		if len(record.Value) == 0 {
			continue
		}

		var obj object

		err = json.Unmarshal(record.Value, &obj)

		if err != nil {
			return fmt.Errorf("getObject ERR: record Unmarshaling ERR: %v", err)
		}

		err = db.AddObject(context.Background(), obj.Path, obj.Object, obj.ID)

		if err != nil {
			return fmt.Errorf("getObject ERR: %v", err)
		}
	}
}
