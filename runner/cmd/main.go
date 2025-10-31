package main

import (
	"fmt"
	"runner/runner"
)

func main() {
	err := runner.Run()

	if err != nil {
		fmt.Printf("runner main ERR: %v", err)
	}
}
