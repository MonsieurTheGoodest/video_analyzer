package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	vidio "github.com/AlexEidt/Vidio"
	"github.com/twmb/franz-go/pkg/kgo"
)

type image struct {
	Frame []byte `json:"frame"`
	Path  string `json:"path"`
	ID    int64  `json:"id"`
	Epoch int64  `json:"epoch"`
}

type Processor struct {
	StartFrame int64  `json:"id"`
	Path       string `json:"path"`
	Epoch      int64  `json:"epoch"`
	stopped    bool
}

type stopping struct {
	StopPath string `json:"stop_path"`
	Epoch    int64  `json:"epoch"`
}

type ending struct {
	EndPath string `json:"end_path"`
	Epoch   int64  `json:"epoch"`
}

func NewProcessor() *Processor {
	return &Processor{}
}

var ErrVideoNotFound error = errors.New("video not found")
var ErrEpochNotEqual error = errors.New("ENE")

const heartbeatInterval = time.Second * 5
const sessionTimeout = time.Second * 45

func (p *Processor) CheckStopping() error {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(fmt.Sprint(os.Getpid())),
		kgo.ConsumeTopics("stop"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return fmt.Errorf("checkStopping ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(context.Background())

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("checkStopping ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				return fmt.Errorf("checkStopping ERR: refusing to send empty value")
			}

			var stp stopping

			err = json.Unmarshal(record.Value, &stp)

			if err != nil {
				return fmt.Errorf("checkStopping ERR: Unmarshaling ERR: %v", err)
			}

			if stp.StopPath == p.Path && stp.Epoch >= p.Epoch && p.Path != "" {
				p.stopped = true
			}

			err = cl.CommitUncommittedOffsets(context.Background())

			if err != nil {
				return fmt.Errorf("checkStopping ERR: commiting offsetts ERR: %v", err)
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

	video := vidio.Video{}
	defer video.Close()

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

			var config Processor

			err = json.Unmarshal(record.Value, &config)

			if err != nil {
				return fmt.Errorf("readPath ERR: record Unmarshaling ERR: %v", err)
			}

			p.Path = config.Path
			p.Epoch = config.Epoch
			p.stopped = false

			fmt.Printf("%s started processing\n", p.Path)

			if _, err := os.Stat(p.Path); errors.Is(err, os.ErrNotExist) {
				fmt.Printf("[WARN] processScenario: video file %s not found, skipping...\n", p.Path)

				delErr := p.deleteScenario()

				if delErr != nil {
					return fmt.Errorf("readPath ERR: %v", delErr)
				}

				continue
			}

			err = p.commitStartOfScenario()

			if errors.Is(err, ErrEpochNotEqual) {
				continue
			}

			if err != nil {
				return fmt.Errorf("readPath ERR: %v", err)
			}

			cl.CommitUncommittedOffsets(context.Background())

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
	)

	if err != nil {
		return fmt.Errorf("commitEndOfScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	b, err := json.Marshal(ending{
		EndPath: p.Path,
		Epoch:   p.Epoch,
	})

	if err != nil {
		return fmt.Errorf("commitEndOfScenario ERR: marshaling ERR: %v", err)
	}

	record := &kgo.Record{
		Topic: "end",
		Value: b,
	}

	if err := cl.ProduceSync(context.Background(), record).FirstErr(); err != nil {
		return fmt.Errorf("commitEndOfScenario ERR: producer sending message ERR: %v", err)
	}

	return nil
}

func (p *Processor) deleteScenario() error {
	seeds := []string{
		"kafka_orchestrator_and_runner1:9092",
		"kafka_orchestrator_and_runner2:9092",
		"kafka_orchestrator_and_runner3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)

	if err != nil {
		return fmt.Errorf("deleteScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	b, err := json.Marshal(p.Path)

	if err != nil {
		return fmt.Errorf("deleteScenario ERR: marshaling ERR: %v", err)
	}

	record := &kgo.Record{
		Topic: "delete",
		Value: b,
	}

	if err := cl.ProduceSync(context.Background(), record).FirstErr(); err != nil {
		return fmt.Errorf("deleteScenario ERR: producer sending message ERR: %v", err)
	}

	return nil
}

func (p *Processor) commitStartOfScenario() error {
	baseURL := "http://orchestrator:9000/start"

	u, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("commitStartOfScenario ERR: parsing baseURL ERR: %v", err)
	}

	q := u.Query()
	q.Set("path", p.Path)
	q.Set("epoch", fmt.Sprintf("%d", p.Epoch))
	u.RawQuery = q.Encode()

	client := &http.Client{Timeout: 5 * time.Second}

	resp, err := client.Post(u.String(), "application/json", nil)
	if err != nil {
		return fmt.Errorf("commitStartOfScenario ERR: sending request ERR: %v", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return ErrVideoNotFound
	case http.StatusConflict:
		return ErrEpochNotEqual
	default:
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commitStartOfScenario ERR: unexpected status %d, body: %s", resp.StatusCode, string(body))
	}

	metaURL := fmt.Sprintf("http://orchestrator:9000/meta?path=%s", url.QueryEscape(p.Path))

	for i := 0; i < 5; i++ {
		time.Sleep(200 * time.Millisecond)

		resp, err := client.Get(metaURL)
		if err != nil {
			continue
		}

		var meta struct {
			Status     string `json:"status"`
			StartFrame int64  `json:"start_frame"`
			Epoch      int64  `json:"epoch"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&meta); err == nil {
			resp.Body.Close()
			if meta.Status == "Running" {
				return nil
			}
		}
		resp.Body.Close()
	}

	return fmt.Errorf("commitStartOfScenario ERR: status not updated to Running after start")
}

func (p *Processor) processScenario() error {
	video, err := vidio.NewVideo(p.Path)

	if err != nil {
		return fmt.Errorf("readPath ERR: receiving path ERR: %v", err)
	}
	defer video.Close()

	seeds := []string{"kafka_runner_to_inference1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.ProducerBatchMaxBytes(100_000_000),
		kgo.MaxBufferedRecords(10),
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

		if id <= p.StartFrame || id%25 != 0 {
			id++

			continue
		}

		frame, err := json.Marshal(image{
			Frame: video.FrameBuffer(),
			Path:  p.Path,
			ID:    id,
			Epoch: p.Epoch,
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
		fmt.Printf("ended scenario %v\n", p.Path)
		err = p.commitEndOfScenario()

		p.stopped = true

		if err != nil {
			return fmt.Errorf("processScenario ERR: %v", err)
		}
	}

	return nil
}
