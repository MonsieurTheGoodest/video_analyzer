package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runner/runner"
	"syscall"
)

type processor interface {
	CheckStopping() error
	ReadPath() error
}

func main() {
	var p processor = runner.NewProcessor()

	errCh := make(chan error, 2)
	defer close(errCh)

	go func() { errCh <- p.CheckStopping() }()
	go func() { errCh <- p.ReadPath() }()

	sigCh := make(chan os.Signal, 1)
	defer close(sigCh)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("shutdown: got signal %s", sig)
	case err := <-errCh:
		if err != nil {
			fmt.Printf("runner main ERR: %v", err)
		}
	}
}
