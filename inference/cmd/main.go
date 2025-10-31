package main

import (
	"fmt"
	"inference/inference"
)

func main() {
	err := inference.Run()

	if err != nil {
		fmt.Printf("inference main ERR: %v", err)
	}
}
