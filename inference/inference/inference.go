package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

type image struct {
	Frame []byte `json:"frame"`
	Path  string `json:"path"`
	ID    int64  `json:"id"`
}

type object struct {
	ID     int64  `json:"id"`
	Path   string `json:"path"`
	Object string `json:"object"`
}

func Run() error {
	for {
		frame, path, id, err := readFrame()

		if err != nil {
			return fmt.Errorf("Run ERR: %v", err)
		}

		object, err := processFrame(frame, id)

		if err != nil {
			return fmt.Errorf("Run ERR: %v", err)
		}

		err = sendObjects(object, path, id)

		if err != nil {
			return fmt.Errorf("Run ERR: %v", err)
		}
	}
}

func readFrame() ([]byte, string, int64, error) {
	seeds := []string{"kafka_runner_to_inference1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("inference"),
		kgo.ConsumeTopics("frames"),
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return []byte{}, "", -1, fmt.Errorf("readFrame ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	fetches := cl.PollFetches(ctx)

	if errs := fetches.Errors(); len(errs) > 0 {
		return []byte{}, "", -1, fmt.Errorf("readFrame ERR: fetching message ERR: %v", errs)
	}

	iter := fetches.RecordIter()

	record := iter.Next()

	var img image

	err = json.Unmarshal(record.Value, &img)

	if err != nil {
		return []byte{}, "", -1, fmt.Errorf("readFrame ERR: record Unmarshaling ERR: %v", err)
	}

	return img.Frame, img.Path, img.ID, nil
}

func processFrame(frame []byte, id int64) (string, error) {
	return strconv.Itoa(int(id)), nil
}

func sendObjects(o, path string, id int64) error {
	seeds := []string{"kafka_inference_to_orchestrator1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return fmt.Errorf("sendObjects ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	obj, err := json.Marshal(object{
		Object: o,
		Path:   path,
		ID:     id,
	})

	if err != nil {
		return fmt.Errorf("sendObjects ERR: record Marshaling ERR: %v", err)
	}

	record := &kgo.Record{
		Topic: "objects",
		Value: obj,
	}

	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("sendObjects ERR: producer sending message ERR: %v", err)
	}

	return nil
}
