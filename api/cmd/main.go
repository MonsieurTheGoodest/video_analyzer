package main

import (
	"api/api"
	"errors"
	"fmt"
)

var errStatus error = errors.New("api main ERR: status should be stop or restart")

const MY_PATH string = "./video/1.mp4"

func main() {
	err := api.ProcessScenario(MY_PATH)

	if err != nil {
		fmt.Printf("api main ERR: %v", err)
		return
	}

	for {
		var op string

		fmt.Print("Input operation: ")
		fmt.Scan(&op)

		var path string

		fmt.Print("Input path: ")
		fmt.Scan(&path)

		switch op {
		case "ProcessScenario":
			err := api.ProcessScenario(path)

			if err != nil {
				fmt.Printf("api main ERR: %v", err)
				return
			}

		case "ChangeStatusScenario":
			var statusToDo string

			fmt.Print("Input statusToDo (stop or restart): ")
			fmt.Scan(&path, &statusToDo)

			if statusToDo != "stop" && statusToDo != "restart" {
				fmt.Print(errStatus)
				return
			}

			err := api.ChangeStatusScenario(path, statusToDo)

			if err != nil {
				fmt.Printf("api main ERR: %v", err)
				return
			}

		case "Objects":
			objects, err := api.Objects(path)

			if err != nil {
				fmt.Printf("api main ERR: %v", err)
			}

			fmt.Println(objects)

		case "Status":
			objects, err := api.Objects(path)

			if err != nil {
				fmt.Printf("api main ERR: %v", err)
			}

			fmt.Println(objects)
		}
	}
}
