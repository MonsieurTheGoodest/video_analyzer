package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProcessScenario(path string) error {
	seeds := []string{
		"kafka_api_to_orchestrator1:9092",
		"kafka_api_to_orchestrator2:9092",
		"kafka_api_to_orchestrator3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return fmt.Errorf("processScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	b, err := json.Marshal(path)

	if err != nil {
		return fmt.Errorf("processScenario ERR: marshaling ERR: %v", err)
	}

	record := &kgo.Record{
		Topic: "path",
		Key:   b,
		Value: b,
	}

	err = cl.ProduceSync(ctx, record).FirstErr()

	if err != nil {
		return fmt.Errorf("processScenario ERR: producer sending message ERR: %v", err)
	}

	return nil
}

func ChangeStatusScenario(path string, status string) error {
	seeds := []string{
		"kafka_api_to_orchestrator1:9092",
		"kafka_api_to_orchestrator2:9092",
		"kafka_api_to_orchestrator3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return fmt.Errorf("changeStatusScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	b, err := json.Marshal(status)

	if err != nil {
		return fmt.Errorf("changeStatusScenario ERR: marshaling ERR: %v", err)
	}

	record := &kgo.Record{
		Topic: "status",
		Key:   b,
		Value: b,
	}

	err = cl.ProduceSync(ctx, record).FirstErr()

	if err != nil {
		return fmt.Errorf("changeStatusScenario ERR: producer sending message ERR: %v", err)
	}

	return nil

}

func Objects(path string) (string, error) {
	return "", nil
}

func Status(path string) (string, error) {
	return "", nil
}
