package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	rand.Seed(time.Now().UnixNano()) // Seed the random generator
	log.Println("Inside UniqueID Generation main")
	n := maelstrom.NewNode()
	processId := os.Getpid()

	// Register the Unique Id generate handler
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as loosely typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Updating the response message type
		body["type"] = "generate_ok"

		body["id"] = fmt.Sprintf("%d-%d", rand.Int63(), processId)

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
