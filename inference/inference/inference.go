package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type image struct {
	Frame []byte `json:"frame"`
	Path  string `json:"path"`
	ID    int64  `json:"id"`
	Epoch int64  `json:"epoch"`
}

type object struct {
	ID     int64  `json:"id"`
	Path   string `json:"path"`
	Object string `json:"object"`
	Epoch  int64  `json:"epoch"`
}

type Processor struct {
	images           chan image
	objects          chan object
	mu               *sync.Mutex
	currentProcesses map[string]int64
	lastPathUsing    map[string]time.Time
}

type stopping struct {
	StopPath string `json:"stop_path"`
	Epoch    int64  `json:"epoch"`
}

const imagesSize = 50
const objectsSize = 50
const maxUnusedTime = time.Minute * 2

func NewProcessor() *Processor {
	return &Processor{
		images:           make(chan image, imagesSize),
		objects:          make(chan object, objectsSize),
		mu:               &sync.Mutex{},
		currentProcesses: make(map[string]int64),
		lastPathUsing:    map[string]time.Time{},
	}
}

func (p *Processor) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()

	del := make([]string, 0)

	for path, lastUsing := range p.lastPathUsing {
		if lastUsing.Add(maxUnusedTime).Before(time.Now()) {
			del = append(del, path)
		}
	}

	for _, path := range del {
		delete(p.currentProcesses, path)
		delete(p.lastPathUsing, path)
	}
}

func (p *Processor) CheckStopping() error {
	seeds := []string{"kafka_runner_to_inference1:9092"}

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

			if _, ok := p.currentProcesses[stp.StopPath]; ok {
				p.currentProcesses[stp.StopPath] = stp.Epoch + 1
			}
		}
	}
}

func (p *Processor) ReadFrame() error {
	seeds := []string{"kafka_runner_to_inference1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("inference"),
		kgo.ConsumeTopics("frames"),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(100_000_000),
	)

	if err != nil {
		return fmt.Errorf("readFrame ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	for {
		fetches := cl.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("readFrame ERR: fetching message ERR: %v", errs)
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			if record == nil || len(record.Value) == 0 {
				return fmt.Errorf("readFrame ERR: refusing to send empty value")
			}

			cl.CommitUncommittedOffsets(context.Background())

			var img image

			err = json.Unmarshal(record.Value, &img)

			if err != nil {
				return fmt.Errorf("readFrame ERR: record Unmarshaling ERR: %v", err)
			}

			if !p.checkEpoch(img) {
				continue
			}

			p.images <- img
		}
	}
}

func (p *Processor) ProcessFrame() error {
	for img := range p.images {
		if !p.checkEpoch(img) {
			continue
		}

		p.objects <- object{
			Path:   img.Path,
			Object: strconv.Itoa(int(img.ID)),
			ID:     img.ID,
			Epoch:  img.Epoch,
		}
	}

	return nil
}

func (p *Processor) SendObjects() error {
	seeds := []string{"kafka_inference_and_orchestrator1:9092"}

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

	for obj := range p.objects {
		objByte, err := json.Marshal(object{
			Object: obj.Object,
			Path:   obj.Path,
			ID:     obj.ID,
			Epoch:  obj.Epoch,
		})

		if err != nil {
			return fmt.Errorf("sendObjects ERR: record Marshaling ERR: %v", err)
		}

		record := &kgo.Record{
			Topic: "objects",
			Value: objByte,
		}

		if err := cl.ProduceSync(ctx, record).FirstErr(); err != nil {
			return fmt.Errorf("sendObjects ERR: producer sending message ERR: %v", err)
		}

	}

	return nil
}

func (p *Processor) Close() {
	close(p.images)
	close(p.objects)
}

func (p *Processor) checkEpoch(img image) bool {
	p.lastPathUsing[img.Path] = time.Now()

	if epoch, ok := p.currentProcesses[img.Path]; ok {
		return img.Epoch >= epoch
	}

	p.currentProcesses[img.Path] = img.Epoch

	return true
}
