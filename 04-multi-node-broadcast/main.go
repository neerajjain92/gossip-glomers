package main

import (
	"encoding/json"
	"log"
	"reflect"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/maps"
)

// DeepCloneMap creates a deep copy of a map[string]interface{}.
func DeepCloneMap(originalMap map[string]interface{}) map[string]interface{} {
	// Marshal the original map into JSON
	mapJSON, err := json.Marshal(originalMap)
	if err != nil {
		log.Fatalf("Error marshalling map: %v", err)
	}

	// Unmarshal the JSON back into a new map
	var clonedMap map[string]interface{}
	err = json.Unmarshal(mapJSON, &clonedMap)
	if err != nil {
		log.Fatalf("Error unmarshalling map: %v", err)
	}

	return clonedMap
}

func main() {
	log.Println("Inside MultiNode Brodcast Main")
	n := maelstrom.NewNode()
	messagesMap := make(map[float64]struct{})
	peers := []string{}

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

		log.Printf("Received Broadcast on %s and the message %v \n", n.ID(), body)

		// Persist the data
		addIfNotPresent(messagesMap, body["message"].(float64))

		// Do the peerCopy
		go initiatePeerCopy(DeepCloneMap(body), peers, n)

		// Do the cleanup
		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})

	n.Handle("peerCopy", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		log.Printf("Received peerCopy message on %s from %s", msg.Dest, msg.Src)
		addIfNotPresent(messagesMap, body["message"].(float64))

		body["type"] = "peerCopyOk"
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
		body["messages"] = maps.Keys(messagesMap)

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

		// Persist the topology as well
		topology := body["topology"].(map[string]interface{})

		log.Println("Actual Topology :==> ", topology)
		log.Printf("And the type of specific value is %v", reflect.TypeOf(topology[n.ID()]))

		// Iterate over topology Interface
		for _, connectionsInterface := range topology {
			// Type assertion for connections
			connections, ok := connectionsInterface.([]interface{})
			if !ok {
				log.Fatal("Tpe assertion Failed for Connections interface")
				panic(nil)
			}
			for _, connection := range connections {
				connectionStr, ok := connection.(string)
				if !ok {
					log.Fatal("Type assertion failed for connection ")
					panic(nil)
				}
				if connectionStr != n.ID() {
					peers = append(peers, connectionStr)
				}
			}
		}

		delete(body, "topology")
		log.Printf("Complete Toplogy of %s is %v", n.ID(), peers)
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func addIfNotPresent(messagesMap map[float64]struct{}, message float64) {
	var exists struct{}
	if _, found := messagesMap[message]; !found {
		messagesMap[message] = exists
	}
}

func initiatePeerCopy(body map[string]any, peers []string, n *maelstrom.Node) {
	peerCopyMessage := map[string]interface{}{
		"type":    "peerCopy",
		"message": body["message"],
		"msg_id":  body["msg_id"],
	}
	for _, peer := range peers {
		n.Send(peer, peerCopyMessage)
	}
}
