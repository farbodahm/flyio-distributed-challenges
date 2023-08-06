package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// n is current node
var n *maelstrom.Node
var kv *maelstrom.KV
var m sync.Mutex
var visited_messages map[int]bool

// add_handler adds the new recieved value to the current state if
// that massage id is not already seen.
func add_handler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "add_ok"

	delta := int(body["delta"].(float64))
	msg_id := int(body["msg_id"].(float64))

	m.Lock()
	if _, ok := visited_messages[msg_id]; ok {
		m.Unlock()
		return n.Reply(msg, response)
	}

	value := get_state()
	value += delta
	update_state(value)
	visited_messages[msg_id] = true
	m.Unlock()

	go broadcast_message(delta, msg_id)

	return n.Reply(msg, response)
}

// read_handler returns the current state.
func read_handler(msg maelstrom.Message) error {
	response := make(map[string]any)
	response["type"] = "read_ok"
	response["value"] = get_state()

	return n.Reply(msg, response)
}

// broadcast_message sends the message to all other nodes.
// If it don't recieve an ack, it will try to send the message again.
func broadcast_message(delta int, msg_id int) error {
	nodes := make([]string, 0, len(n.NodeIDs())-1)
	for _, node := range n.NodeIDs() {
		if node != n.ID() {
			nodes = append(nodes, node)
		}
	}

	for len(nodes) > 0 {
		node := nodes[0]
		nodes = nodes[1:]

		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
		defer cancelFunc()

		body := make(map[string]any)
		body["type"] = "add"
		body["delta"] = delta
		body["msg_id"] = msg_id

		// if couldn't receive ack, add value back to the queue
		msg, err := n.SyncRPC(ctx, node, body)
		if err != nil {
			nodes = append(nodes, node)
			continue
		}
		var response map[string]any
		if err := json.Unmarshal(msg.Body, &response); err != nil {
			return err
		}
		if response["type"] != "add_ok" {
			log.Println("Unexpected response:", response)
			return errors.New(fmt.Sprint("Unexpected response:", response))
		}
	}

	return nil
}

// get_state returns the current value saved in the state store. It will return 0 if state store is empty and initialize it.
// in case of time out, it tries to reach the state store again.
func get_state() int {
	for {
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
		defer cancelFunc()

		value, err := kv.ReadInt(ctx, "state")

		if err != nil {
			if rpcError, ok := err.(*maelstrom.RPCError); ok && rpcError.Code == maelstrom.KeyDoesNotExist {
				log.Println("Initializing state with 0")
				update_state(0)
				return 0
			}

			// if context deadline exceeded, try reaching state store again
			// it can happen because of network partition
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			log.Panicln("Read State failed:", err)
		}
		return value
	}
}

// update_state saves the new value in the state store.
// in case of time out, it tries to reach the state store again.
func update_state(value int) {
	for {
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*2)
		defer cancelFunc()

		err := kv.Write(ctx, "state", value)
		if err == nil {
			break
		}

		// if context deadline exceeded, try reaching state store again
		// it can happen because of network partition
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}

		log.Panicln("Update State failed:", err)
	}
}

func main() {

	n = maelstrom.NewNode()
	kv = maelstrom.NewSeqKV(n)
	visited_messages = make(map[int]bool)

	n.Handle("add", add_handler)
	n.Handle("read", read_handler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
