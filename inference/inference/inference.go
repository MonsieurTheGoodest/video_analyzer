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

type Processor struct {
	images  chan image
	objects chan object
}

const imagesSize = 50
const objectsSize = 50

func NewProcessor() *Processor {
	return &Processor{
		images:  make(chan image, imagesSize),
		objects: make(chan object, objectsSize),
	}
}

func (p *Processor) close() {
	close(p.images)
	close(p.objects)
}

func (p *Processor) ReadFrame() error {
	defer p.close()

	seeds := []string{"kafka_runner_to_inference1:9092"}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("inference"),
		kgo.ConsumeTopics("frames"),
		//kgo.AllowAutoTopicCreation(),
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

			p.images <- img
		}
	}
}

func (p *Processor) ProcessFrame() error {
	defer p.close()

	for img := range p.images {
		p.objects <- object{
			Path:   img.Path,
			Object: strconv.Itoa(int(img.ID)),
			ID:     img.ID,
		}
	}

	return nil
}

func (p *Processor) SendObjects() error {
	defer p.close()

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

	for obj := range p.objects {
		objByte, err := json.Marshal(object{
			Object: obj.Object,
			Path:   obj.Path,
			ID:     obj.ID,
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
