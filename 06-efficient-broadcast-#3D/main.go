package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/btree"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const maxRetry = 100

func main() {
	n := maelstrom.NewNode()
	s := &Server{n: n, ids: make(map[int]struct{})}

	n.Handle("init", s.initHandler)
	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Server struct {
	n      *maelstrom.Node
	nodeId string
	id     int

	idsMutex sync.RWMutex
	ids      map[int]struct{}

	nodesMutex sync.RWMutex
	topology   *btree.Tree
}

func (s *Server) initHandler(_ maelstrom.Message) error {
	s.nodeId = s.n.ID()
	// Get the numeric part from the Id
	// So if the nodes are named as n1, n2, n3
	// let's fetch 1, 2, and 3 respectively
	id, err := strconv.Atoi(s.nodeId[1:])
	if err != nil {
		return err
	}
	s.id = id
	log.Printf("Initializing node with nodeId %s and id %d", s.nodeId, id)
	return nil
}

func (s *Server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go func() {
		_ = s.n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
		log.Printf("Received broadcast %v", body)
	}()

	message := int(body["message"].(float64))
	s.idsMutex.Lock()
	if _, exists := s.ids[message]; exists {
		s.idsMutex.Unlock()
		return nil
	}

	s.ids[message] = struct{}{}
	s.idsMutex.Unlock()
	return s.peerCopy(msg.Src, body)
}

func (s *Server) peerCopy(src string, body map[string]any) error {
	s.nodesMutex.RLock()
	n := s.topology.GetNode(s.id)
	defer s.nodesMutex.RUnlock()

	// Since we are only keeping 2 level of tree, So for a topology of 5 nodes
	// Here is how are btree will look like
	//
	//                   [3]  --------------> Root
	//                  //  \\
	//               [0, 1] [4, 5] ------------> Leaf
	// All leaf will only peer-copy to parent
	// Only parent will peer-copy to child

	var neighbours []string
	// All child will peer-copy to root node
	if n.Parent != nil {
		neighbours = append(neighbours, n.Parent.Entries[0].Value.(string))
	}

	// Now iterate through all children
	//
	for _, children := range n.Children {
		for _, entry := range children.Entries {
			neighbours = append(neighbours, entry.Value.(string))
		}
	}

	log.Printf("Neighbours of node %v are %v", n, neighbours)

	for _, dst := range neighbours {
		if dst == src || dst == s.nodeId {
			continue // Skip PeerCopy to self or from the node where message came from
		}

		dst := dst
		go func() {
			if err := s.initiateRPC(dst, body); err != nil {
				for i := 0; i < maxRetry; i++ {
					// Retry with backoff
					if err := s.initiateRPC(dst, body); err != nil {
						// Sleep and retry with a jitter
						// Sleep for 1 second in 1st round, 2 in 2nd, 3 in 3rd and so on
						time.Sleep(time.Duration(i) * time.Second)
						continue
					}
					return
				}
				log.Println(err)
			}
		}()
	}
	return nil
}

func (s *Server) initiateRPC(dst string, body map[string]any) error {
	// Cancel after 1 second
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.n.SyncRPC(ctx, dst, body)
	return err
}

func (s *Server) readHandler(msg maelstrom.Message) error {
	messages := s.getAllMessages()
	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

// Helper to create a deep-clone of our messages
func (s *Server) getAllMessages() []int {
	s.idsMutex.RLock() // Only read lock is necessary
	messages := make([]int, 0, len(s.ids))
	for message := range s.ids {
		messages = append(messages, message)
	}
	s.idsMutex.RUnlock()
	return messages
}

func (s *Server) topologyHandler(msg maelstrom.Message) error {
	// In Btree if the order is 't'
	// Then any node can have max t children and t-1 keys
	// So for 25 node cluster
	// There will be 1 root node with 1 key
	// and remaining 1 child with 24 keys
	topologyTree := btree.NewWithIntComparator(len(s.n.NodeIDs()))
	for i := 0; i < len(s.n.NodeIDs()); i++ {
		topologyTree.Put(i, fmt.Sprintf("n%d", i))
	}
	s.nodesMutex.Lock()
	s.topology = topologyTree
	s.nodesMutex.Unlock()
	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
