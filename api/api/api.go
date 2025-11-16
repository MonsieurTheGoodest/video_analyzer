package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Meta struct {
	Status     string `json:"status"`
	StartFrame int64  `json:"start_frame"`
	Epoch      int64  `json:"epoch"`
}

type ObjectResponse struct {
	Object string `json:"object"`
}

const orchestratorBase = "http://orchestrator:9000"

func ProcessScenario(path string) error {
	seeds := []string{
		"kafka_api_to_orchestrator1:9092",
		"kafka_api_to_orchestrator2:9092",
		"kafka_api_to_orchestrator3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
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
	if status != "stop" {
		return fmt.Errorf("changeStatusScenario should be 'stop' ERR")
	}

	seeds := []string{
		"kafka_api_to_orchestrator1:9092",
		"kafka_api_to_orchestrator2:9092",
		"kafka_api_to_orchestrator3:9092",
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)

	if err != nil {
		return fmt.Errorf("changeStatusScenario ERR: client creating ERR: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	b, err := json.Marshal(path)

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

func ProcessScenarioHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing 'path' parameter", http.StatusBadRequest)
		return
	}

	if err := ProcessScenario(path); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func ChangeStatusHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	status := r.URL.Query().Get("status")

	if path == "" || status == "" {
		http.Error(w, "missing 'path' or 'status' parameter", http.StatusBadRequest)
		return
	}

	if err := ChangeStatusScenario(path, status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func GetObjectsHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing 'path' parameter", http.StatusBadRequest)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/object?path=%s", orchestratorBase, path))
	if err != nil {
		http.Error(w, fmt.Sprintf("error contacting orchestrator: %v", err), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write([]byte(readBody(resp)))
}

func GetMetaHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing 'path' parameter", http.StatusBadRequest)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/meta?path=%s", orchestratorBase, path))
	if err != nil {
		http.Error(w, fmt.Sprintf("error contacting orchestrator: %v", err), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write([]byte(readBody(resp)))
}

func readBody(resp *http.Response) string {
	defer resp.Body.Close()
	buf := make([]byte, resp.ContentLength)
	resp.Body.Read(buf)
	return string(buf)
}

func GetCurrentProcessesHandler(w http.ResponseWriter, r *http.Request) {
	resp, err := http.Get(fmt.Sprintf("%s/processes", orchestratorBase))
	if err != nil {
		http.Error(w, fmt.Sprintf("error contacting orchestrator: %v", err), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write([]byte(readBody(resp)))
}
