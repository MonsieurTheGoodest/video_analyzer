package runner

import (
	"context"
	"encoding/json"
	"fmt"

	vidio "github.com/AlexEidt/Vidio"
	"github.com/twmb/franz-go/pkg/kgo"
)

var stopped bool

type videoConfig struct {
	ID   int64  `json:"id"`
	Path string `json:"path"`
}

type image struct {
	Frame []byte `json:"frame"`
	Path  string `json:"path"`
	ID    int64  `json:"id"`
}

func Run() error {
	for {
		path, startFrame, err := readPath()

		if err != nil {
			return fmt.Errorf("Run ERR: %v", err)
		}

		var stopping_err error

		go func() {
			stopping_err = checkStopping(path)

			fmt.Print("Run ERR: stopping ERR: ")
		}()

		err = processScenario(path, startFrame)

		if err != nil {
			return fmt.Errorf("Run ERR: %v", err)
		}

		if stopping_err != nil {
			stopped = true

			return fmt.Errorf("Run ERR: stopping ERR: %v", stopping_err)
		}
	}
}

func checkStopping(path string) error {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("checkStopping"),
		kgo.ConsumeTopics("stop"),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return fmt.Errorf("checkStopping ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		kafka_ctx := context.Background()

		fetches := cl.PollFetches(kafka_ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("checkStopping ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()
		record := iter.Next()

		var stoppedID string

		err = json.Unmarshal(record.Value, &stoppedID)

		if err != nil {
			return fmt.Errorf("checkStopping ERR: Unmarshaling ERR: %v", err)
		}

		if stoppedID == path {
			stopped = true

			err = cl.CommitUncommittedOffsets(context.Background())

			if err != nil {
				return fmt.Errorf("checkStopping ERR: commiting offsetts ERR: %v", err)
			}

			break
		}
	}

	return nil
}

func readPath() (string, int64, error) {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("runner"),
		kgo.AllowAutoTopicCreation(),
		kgo.ConsumeTopics("path"),

		//todo
		//kgo.HeartbeatInterval(time.Second*3),
		//kgo.SessionTimeout(time.Second*15),
	)

	if err != nil {
		return "", -1, fmt.Errorf("readPath ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	fetches := cl.PollFetches(ctx)

	if errs := fetches.Errors(); len(errs) > 0 {
		return "", -1, fmt.Errorf("readPath ERR: fetching message ERR: %v", errs)
	}

	iter := fetches.RecordIter()
	record := iter.Next()

	var config videoConfig

	err = json.Unmarshal(record.Value, &config)

	if err != nil {
		return "", -1, fmt.Errorf("readPath ERR: record Unmarshaling ERR: %v", err)
	}

	return config.Path, config.ID, nil
}

func commitEndOfScenario(path string) error {
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
		return fmt.Errorf("commitEndOfScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	b, err := json.Marshal(path)

	if err != nil {
		return fmt.Errorf("commitEndOfScenario ERR: marshaling ERR: %v", err)
	}

	record := &kgo.Record{
		Topic: "end",
		Value: b,
	}

	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("commitEndOfScenario ERR: producer sending message ERR: %v", err)
	}

	return nil
}

func processScenario(path string, startFrame int64) error {
	video, err := vidio.NewVideo(path)

	if err != nil {
		return fmt.Errorf("processScenario ERR: receiving path ERR: %v", err)
	}

	defer video.Close()

	seeds := []string{"kafka_runner_to_inference1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return fmt.Errorf("processScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	var id int64

	for video.Read() {
		if stopped {
			break
		}

		if id < startFrame {
			id++

			continue
		}

		frame, err := json.Marshal(image{
			Frame: video.FrameBuffer(),
			Path:  path,
			ID:    id,
		})

		if err != nil {
			return fmt.Errorf("processScenario ERR: record Marshaling ERR: %v", err)
		}

		record := &kgo.Record{
			Topic: "frames",
			Value: frame,
		}

		if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
			return fmt.Errorf("processScenario ERR: producer sending message ERR: %v", err)
		}
	}

	if !stopped {
		err = commitEndOfScenario(path)

		if err != nil {
			return fmt.Errorf("processScenario ERR: %v", err)
		}
	}

	return nil
}
