package main

import (
	"context"
	"fmt"
	"log"
	"orchestrator/db"
	"orchestrator/orchestrator"
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

	errCh := make(chan error, 3)

	go func() { errCh <- worker.WorkMessage(dataBase) }()
	go func() { errCh <- worker.WorkStatus(dataBase) }()
	go func() { errCh <- worker.WorkEnd(dataBase) }()

	go func() { errCh <- orchestrator.ProcessScenario(dataBase) }()
	go func() { errCh <- orchestrator.ChangeStatus(dataBase) }()
	go func() { errCh <- orchestrator.GetObject(dataBase) }()

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
