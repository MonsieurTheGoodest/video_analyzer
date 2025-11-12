package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	vidio "github.com/AlexEidt/Vidio"
	"github.com/twmb/franz-go/pkg/kgo"
)

type image struct {
	Frame []byte `json:"frame"`
	Path  string `json:"path"`
	ID    int64  `json:"id"`
}

type Processor struct {
	StartFrame int64  `json:"id"`
	Path       string `json:"path"`
	Epoch      int64  `json:"epoch"`
	stopped    bool
}

type stopping struct {
	StopPath string `json:"stop_path"`
	Key      string `json:"key"`
	Epoch    int64  `json:"epoch"`
	tm       time.Time
}

func (s *stopping) isEqual(s1 *stopping) bool {
	return s.StopPath == s1.StopPath && s.Key == s1.Key && s.Epoch == s1.Epoch
}

func NewProcessor() *Processor {
	return &Processor{}
}

const skipTime = time.Second * 5
const heartbeatInterval = time.Second * 3
const sessionTimeout = time.Second * 15

func (p *Processor) CheckStopping() error {
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

	lastStopping := stopping{}

	for {
		fetches := cl.PollFetches(context.Background())

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("checkStopping ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				return fmt.Errorf("checkStopping ERR: refusing to send empty value")
			}

			var stp stopping

			err = json.Unmarshal(record.Value, &stp)

			if err != nil {
				return fmt.Errorf("checkStopping ERR: Unmarshaling ERR: %v", err)
			}

			if stp.StopPath == p.Path && stp.Epoch >= p.Epoch {
				p.stopped = true

				err = cl.CommitUncommittedOffsets(context.Background())

				if err != nil {
					return fmt.Errorf("checkStopping ERR: commiting offsetts ERR: %v", err)
				}

				break
			}

			if !stp.isEqual(&lastStopping) {
				lastStopping = stp
				lastStopping.tm = time.Now()
			} else if lastStopping.tm.Add(skipTime).Before(time.Now()) && p.Path != "" {
				err = cl.CommitUncommittedOffsets(context.Background())

				if err != nil {
					return fmt.Errorf("checkStopping ERR: commiting offsetts ERR: %v", err)
				}

				break
			}
		}
	}
}

func (p *Processor) ReadPath() error {
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
		kgo.DisableAutoCommit(),
		kgo.HeartbeatInterval(heartbeatInterval),
		kgo.SessionTimeout(sessionTimeout),
	)

	if err != nil {
		return fmt.Errorf("readPath ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	for {
		fetches := cl.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("readPath ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				continue
			}

			cl.CommitUncommittedOffsets(context.Background())

			var config Processor

			err = json.Unmarshal(record.Value, &config)

			if err != nil {
				return fmt.Errorf("readPath ERR: record Unmarshaling ERR: %v", err)
			}

			err = p.commitStartOfScenario()

			if err != nil {
				return fmt.Errorf("readPath ERR: %v", err)
			}

			p.Path = config.Path
			p.stopped = false

			err = p.processScenario()

			if err != nil {
				return fmt.Errorf("readPath ERR: %v", err)
			}
		}
	}
}

func (p *Processor) commitEndOfScenario() error {
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

	b, err := json.Marshal(p.Path)

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

func (p *Processor) commitStartOfScenario() error {
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
		return fmt.Errorf("commitStartOfScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	b, err := json.Marshal(p.Path)

	if err != nil {
		return fmt.Errorf("commitStartOfScenario ERR: marshaling ERR: %v", err)
	}

	record := &kgo.Record{
		Topic: "start",
		Value: b,
	}

	if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("commitStartOfScenario ERR: producer sending message ERR: %v", err)
	}

	return nil
}

func (p *Processor) processScenario() error {
	video, err := vidio.NewVideo(p.Path)

	if err != nil {
		return fmt.Errorf("processScenario ERR: receiving path ERR: %v", err)
	}

	defer video.Close()

	seeds := []string{"kafka_runner_to_inference1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.ProducerBatchMaxBytes(100_000_000),
		kgo.MaxBufferedRecords(10),
		//kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		return fmt.Errorf("processScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	var id int64

	for video.Read() {
		if p.stopped {
			break
		}

		if id <= p.StartFrame {
			id++

			continue
		}

		frame, err := json.Marshal(image{
			Frame: video.FrameBuffer(),
			Path:  p.Path,
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

		id++
	}

	if !p.stopped {
		err = p.commitEndOfScenario()

		p.stopped = true

		if err != nil {
			return fmt.Errorf("processScenario ERR: %v", err)
		}
	}

	return nil
}
