package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var messages []int
var n *maelstrom.Node

// broadcast_to_other_nodes sends the recieved new message to all other nodes.
func broadcast_to_other_nodes(new_id int) error {
	response := make(map[string]any)
	response["type"] = "node_broadcast"
	response["message"] = new_id

	for _, node := range n.NodeIDs() {
		if node != n.ID() {
			if err := n.Send(node, response); err != nil {
				return err
			}
		}
	}

	return nil
}

// node_broadcast_handler adds the recieved message to the list of recieved messages.
func node_broadcast_handler(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	new_id := int(body["message"].(float64))
	messages = append(messages, new_id)

	return nil
}

// broadcast_handler adds the recieved message to the list of recieved messages
// and also broadcast it to other nodes.
func broadcast_handler(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	new_id := int(body["message"].(float64))
	messages = append(messages, new_id)

	if err := broadcast_to_other_nodes(new_id); err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "broadcast_ok"

	return n.Reply(msg, response)
}

// read_handler returns the list of recieved messages.
func read_handler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	body["messages"] = messages

	return n.Reply(msg, body)
}

// topology_handler
func topology_handler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "topology_ok"

	return n.Reply(msg, response)
}

func main() {
	n = maelstrom.NewNode()

	n.Handle("broadcast", broadcast_handler)
	n.Handle("read", read_handler)
	n.Handle("topology", topology_handler)
	n.Handle("node_broadcast", node_broadcast_handler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
