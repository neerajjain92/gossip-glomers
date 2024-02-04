package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	log.Println("Inside Echo Main")
	n := maelstrom.NewNode()
	messages := []float64{}

	// Register the broadcast handler
	/**
	// Sample
	{
	  "type": "broadcast",
	  "message": 1000
	}
	**/
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Persist the data
		messages = append(messages, body["message"].(float64))
		// Store whatever we got into in-memory storage to be read later by caller
		body["type"] = "broadcast_ok"
		delete(body, "message")

		return n.Reply(msg, body)
	})

	// Handle Read operation
	/**
	Request
	{
		"type": "read"
	}

	Response
	{
	  "type": "read_ok",
	  "messages": [1, 8, 72, 25]
	}
	**/

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	// Handle the topology request
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		log.Println("Topology is >>>>>>>>>>>>>>>>>", body)

		body["type"] = "topology_ok"
		delete(body, "topology")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
