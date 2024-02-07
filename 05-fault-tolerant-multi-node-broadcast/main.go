package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

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

// Global variable declaration
var ticker *time.Ticker
var mu sync.Mutex

func main() {
	log.Println("Inside Fault Tolerant MultiNode Brodcast Main")
	ticker = time.NewTicker(400 * time.Millisecond)
	n := maelstrom.NewNode()
	messagesMap := make(map[interface{}]interface{})
	messagesTillNow := []float64{}
	peers := make(map[interface{}]interface{})

	// We should also maintain a map that for every peer, how much we have already peer-copied to them
	peersCheckPoint := make(map[interface{}]map[interface{}]interface{})

	go func() {
		for t := range ticker.C {
			log.Printf("Initiating PeerCopy at %v \n", t)
			initiatePeerCopy(peers, n, peersCheckPoint, messagesTillNow)
		}
	}()

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
		mu.Lock()
		isAdded := addIfNotPresent(messagesMap, body["message"].(float64))
		mu.Unlock()
		if isAdded {
			mu.Lock()
			messagesTillNow = append(messagesTillNow, body["message"].(float64))
			mu.Unlock()
		}

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

		log.Printf("Received peerCopy message %v on %s from %s", body, msg.Dest, msg.Src)
		for _, message := range body["message"].([]interface{}) {
			mu.Lock()
			isAdded := addIfNotPresent(messagesMap, message.(float64))
			mu.Unlock()
			if isAdded {
				mu.Lock()
				messagesTillNow = append(messagesTillNow, message.(float64))
				mu.Unlock()
			} else {
				log.Printf("Skipping message %d from peerCopy", message)
			}
		}

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
		body["messages"] = messagesTillNow

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

		// Iterate over topology Interface, for this Node
		for _, connection := range topology[n.ID()].([]interface{}) {
			connectionStr, ok := connection.(string)
			if !ok {
				log.Fatal("Type assertion failed for connection ")
				panic(nil)
			}
			if connectionStr != n.ID() {
				mu.Lock()
				addIfNotPresent(peers, connectionStr)
				mu.Unlock()
			}
		}

		delete(body, "topology")
		log.Printf("Toplogy of %s is %v", n.ID(), peers)

		// Set the checkpoint for all peers
		for _, peer := range peers {
			peersCheckPoint[peer] = make(map[interface{}]interface{})
		}
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func addIfNotPresent(messagesMap map[interface{}]interface{}, message interface{}) bool {
	var exists struct{}
	if _, found := messagesMap[message]; !found {
		messagesMap[message] = exists
		return true
	}
	return false
}

func initiatePeerCopy(peers map[interface{}]interface{}, n *maelstrom.Node,
	peersCheckPoint map[interface{}]map[interface{}]interface{}, messagesTillNow []float64) {
	peerCopyMessage := map[string]interface{}{
		"type":    "peerCopy",
		"message": []float64{},
	}
	log.Printf("PeersCheckPoint :==> %v \n", peersCheckPoint)
	for _, peer := range maps.Keys(peers) {
		log.Printf("Sending PeerCopy to %v", peer)

		// Find the difference between last checkPoint and current length
		if (len(messagesTillNow) - len(peersCheckPoint[peer])) > 0 {
			// Take the sub-slice and send to peer
			peerCopyMessage["message"] = messagesTillNow[len(peersCheckPoint[peer]):]

			if len(peerCopyMessage["message"].([]float64)) == 0 {
				return
			}

			log.Printf("This peer is lagging behind sending, remaining messsages in one shot, peer: %d; payload: %v \n", peer, peerCopyMessage)

			err := n.RPC(peer.(string), peerCopyMessage, func(msg maelstrom.Message) error {
				var body map[string]any

				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				log.Printf("Received Response of PeerCopy from peer %s; resp=%v", msg.Src, body)

				// Let's add whatever we sent till now
				mu.Lock()
				log.Printf("Updating PeerCheckPoint for peer:%s from %d", msg.Src, len(peersCheckPoint[msg.Src]))
				for _, message := range body["message"].([]interface{}) {
					if peersCheckPoint[msg.Src] == nil {
						peersCheckPoint[msg.Src] = make(map[interface{}]interface{})
					}
					peersCheckPoint[msg.Src][message] = struct{}{}
				}
				log.Printf("Updated PeerCheckPoint for peer:%s to %d", msg.Src, len(peersCheckPoint[msg.Src]))
				log.Printf("Final PeerCheckPoint for peer:%s  [%v]", msg.Src, peersCheckPoint[msg.Src])
				mu.Unlock()
				return nil
			})
			if err != nil {
				log.Printf("Error while sending RPC to peer %s and err=%v \n", peer, err)
			}
		}
	}
}
