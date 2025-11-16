package main

import (
	"context"
	"fmt"
	"log"
	"orchestrator/db"
	healthcheck "orchestrator/health_check"
	"orchestrator/orchestrator"
	"orchestrator/route"
	"orchestrator/server"
	"orchestrator/service"
	"orchestrator/worker"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	dataBase, err := db.NewDataBase(context.Background())

	if err != nil {
		fmt.Print(err)
		return
	}
	defer dataBase.Close()

	errCh := make(chan error, 8)

	router := route.NewRouter(service.NewService(dataBase))

	server := server.NewServer(":9000", router.Router)

	go server.Run()

	go func() { errCh <- healthcheck.HealthCheck(dataBase) }()

	go func() { errCh <- worker.WorkMessage(dataBase) }()
	go func() { errCh <- worker.WorkStatus(dataBase) }()

	go func() { errCh <- orchestrator.ProcessScenario(dataBase) }()
	go func() { errCh <- orchestrator.ChangeStatus(dataBase) }()
	go func() { errCh <- orchestrator.GetObject(dataBase) }()
	go func() { errCh <- orchestrator.SetEnd(dataBase) }()
	go func() { errCh <- orchestrator.DeleteScenario(dataBase) }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("shutdown: got signal %s", sig)
	case err := <-errCh:
		if err != nil {
			log.Printf("worker exited with error: %v", err)
		} else {
			log.Printf("worker exited")
		}
	}
}
