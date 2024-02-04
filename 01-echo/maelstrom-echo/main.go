package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	log.Println("Inside Echo Main")
	n := maelstrom.NewNode()

	// Register the echo  handler
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type
		return n.Reply(msg, body)
	})

	// Let's run  the node
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
