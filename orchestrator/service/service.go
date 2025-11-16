package service

import (
	"context"
	"encoding/json"
	"net/http"
	"orchestrator/db"
	"strconv"
)

type Service struct {
	dataBase DBInterface
}

type DBInterface interface {
	GetVideoObject(ctx context.Context, path string) (object string, err error)
	GetVideoMeta(ctx context.Context, path string) (status string, startFrame int64, epoch int64, err error)
	SetStartOfScenario(ctx context.Context, path string, epoch int64) error
	GetCurrentProcesses(ctx context.Context) ([]string, error)
}

type meta struct {
	Status     string `json:"status"`
	StartFrame int64  `json:"start_frame"`
	Epoch      int64  `json:"epoch"`
}

func NewService(dataBase *db.DataBase) *Service {
	var DBInterface DBInterface = dataBase
	return &Service{dataBase: DBInterface}
}

func (s *Service) GeterVideoObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing 'path' parameter", http.StatusBadRequest)
		return
	}

	object, err := s.dataBase.GetVideoObject(ctx, path)
	if err != nil {
		http.Error(w, "error fetching object: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"object": object,
	})
}

func (s *Service) GeterVideoMeta(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing 'path' parameter", http.StatusBadRequest)
		return
	}

	status, startFrame, epoch, err := s.dataBase.GetVideoMeta(ctx, path)
	if err != nil {
		http.Error(w, "error fetching meta: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta{
		Status:     status,
		StartFrame: startFrame,
		Epoch:      epoch,
	})
}

func (s *Service) SeterStartOfScenario(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := r.URL.Query().Get("path")
	epochStr := r.URL.Query().Get("epoch")

	if path == "" || epochStr == "" {
		http.Error(w, "missing 'path' or 'epoch' parameter", http.StatusBadRequest)
		return
	}

	epoch, err := strconv.ParseInt(epochStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid 'epoch' parameter", http.StatusBadRequest)
		return
	}

	err = s.dataBase.SetStartOfScenario(ctx, path, epoch)
	if err != nil {
		if err.Error() == "ENE" {
			http.Error(w, "epoch not equal or video ended", http.StatusConflict)
			return
		}
		http.Error(w, "error setting start of scenario: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

func (s *Service) GeterCurrentProcesses(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	paths, err := s.dataBase.GetCurrentProcesses(ctx)
	if err != nil {
		http.Error(w, "error fetching current processes: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string][]string{
		"paths": paths,
	})
}
