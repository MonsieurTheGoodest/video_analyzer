package main

import (
	"fmt"
	"inference/inference"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type processor interface {
	ReadFrame() error
	ProcessFrame() error
	SendObjects() error
	CheckStopping() error
	Clear()
}

const clearTime = time.Minute * 10

func main() {
	var p processor = inference.NewProcessor()

	errCh := make(chan error, 4)
	defer close(errCh)

	go func() { errCh <- p.ProcessFrame() }()
	go func() { errCh <- p.ReadFrame() }()
	go func() { errCh <- p.SendObjects() }()
	go func() { errCh <- p.CheckStopping() }()

	ticker := time.NewTicker(clearTime)
	defer ticker.Stop()

	go func() {
		for {
			<-ticker.C
			p.Clear()
		}
	}()

	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("shutdown: got signal %s", sig)
	case err := <-errCh:
		if err != nil {
			fmt.Printf("inference main ERR: %v", err)
		}
	}
}
