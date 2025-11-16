package main

import (
	"log"
	"net/http"

	"api/api"
)

func main() {
	http.HandleFunc("/process", api.ProcessScenarioHandler)
	http.HandleFunc("/status", api.ChangeStatusHandler)
	http.HandleFunc("/object", api.GetObjectsHandler)
	http.HandleFunc("/meta", api.GetMetaHandler)
	http.HandleFunc("/processes", api.GetCurrentProcessesHandler)

	if err := http.ListenAndServe(":9000", nil); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
