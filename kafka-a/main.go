package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// n is current node
var n *maelstrom.Node

// after commiting an offset, it will be pushed from staging state to commited state
var staging_state_store map[string][]int
var commited_state_store map[string][]int

var m sync.RWMutex

// handle_send adds the new value to staging state store and returns the offset
func handle_send(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	value := int(body["msg"].(float64))

	m.Lock()
	staging_state_store[key] = append(staging_state_store[key], value)

	response := make(map[string]any)
	response["type"] = "send_ok"
	response["offset"] = len(staging_state_store[key]) - 1
	m.Unlock()

	return n.Reply(msg, response)
}

// handle_poll returns the staging values for the given key and offset to the
// last offset for the given key
func handle_poll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets_interface := body["offsets"].(map[string]interface{})
	offsets := make(map[string]int)
	for k, v := range offsets_interface {
		offsets[k] = int(v.(float64))
	}

	msgs := make(map[string][][]int)
	m.RLock()
	for k, v := range offsets {
		for i := v; i < len(staging_state_store[k]); i++ {
			msg := []int{i, staging_state_store[k][i]}
			msgs[k] = append(msgs[k], msg)
		}
	}
	m.RUnlock()

	response := make(map[string]any)
	response["type"] = "poll_ok"
	response["msgs"] = msgs

	return n.Reply(msg, response)
}

// handle_commit_offsets commits the offsets till the given offset and the given keys
func handle_commit_offsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets_interface := body["offsets"].(map[string]interface{})
	offsets := make(map[string]int)
	for k, v := range offsets_interface {
		offsets[k] = int(v.(float64))
	}

	m.Lock()
	for k, v := range offsets {
		// Start from latest un-commited offset and commit till the given offset
		for i := len(commited_state_store[k]); i <= v; i++ {
			commited_state_store[k] = append(commited_state_store[k], staging_state_store[k][i])
		}
	}
	m.Unlock()

	response := make(map[string]any)
	response["type"] = "commit_offsets_ok"

	return n.Reply(msg, response)
}

// handle_list_committed_offsets returns the latest commited offset for the given keys
func handle_list_committed_offsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	keys_interface := body["keys"].([]interface{})
	keys := make([]string, 0, len(keys_interface))
	for _, v := range keys_interface {
		keys = append(keys, v.(string))
	}

	commited_offsets := make(map[string]int)
	m.Lock()
	for _, k := range keys {
		if offsets, ok := commited_state_store[k]; ok {
			commited_offsets[k] = len(offsets)
		}
	}
	m.Unlock()

	response := make(map[string]any)
	response["type"] = "list_committed_offsets_ok"
	response["offsets"] = commited_offsets

	return n.Reply(msg, response)
}

func main() {
	n = maelstrom.NewNode()
	staging_state_store = make(map[string][]int)
	commited_state_store = make(map[string][]int)

	n.Handle("send", handle_send)
	n.Handle("poll", handle_poll)
	n.Handle("commit_offsets", handle_commit_offsets)
	n.Handle("list_committed_offsets", handle_list_committed_offsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
