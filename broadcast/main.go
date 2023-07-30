package main

import (
	"encoding/json"
	"log"
	"sort"

	mapset "github.com/deckarep/golang-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var n *maelstrom.Node
var messages mapset.Set[int]
var neighbours_seen map[string]mapset.Set[int]
var neighbours []string

// propagate_to_other_nodes sends the recieved new message to all other nodes.
func propagate_to_other_nodes() error {
	response := make(map[string]any)
	response["type"] = "node_propagate"

	for node, n_seen := range neighbours_seen {
		new_messages := messages.Difference(n_seen).ToSlice()

		if len(new_messages) == 0 {
			continue
		}
		response["message"] = new_messages
		log.Println(n.ID(), " sending to ", node, " msg: ", new_messages)

		if err := n.RPC(node, response, func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}
			log.Printf("%s Send to %s successfully\n", n.ID(), msg.Src)
			if body["type"] == "node_propagate_ok" {
				// neighbours_seen[node].Append(new_messages...)
				received_messages := body["received_messages"].([]interface{})
				received_messages_int := make([]int, len(received_messages))
				for i, v := range received_messages {
					received_messages_int[i] = int(v.(float64))
				}
				log.Println(node, " replied ok for", received_messages_int, "total message", body)
				neighbours_seen[node].Append(received_messages_int...)
			}
			return nil
		}); err != nil {
			log.Println("fuck error!")
		}
	}

	return nil
}

// node_propagate_handler adds the recieved message to the list of recieved messages.
func node_propagate_handler(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	new_messages := body["message"].([]interface{})
	new_messages_int := make([]int, len(new_messages))
	for i, v := range new_messages {
		new_messages_int[i] = int(v.(float64))
	}
	log.Println(n.ID(), "got", new_messages_int)

	if n := messages.Append(new_messages_int...); n > 0 {
		propagate_to_other_nodes()
	}

	response := make(map[string]any)
	response["type"] = "node_propagate_ok"
	response["received_messages"] = new_messages_int
	log.Println(n.ID(), "replying is", response)
	return n.Reply(msg, response)
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
	messages.Add(new_id)

	if err := propagate_to_other_nodes(); err != nil {
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
	body["messages"] = messages.ToSlice()
	sort.Ints(body["messages"].([]int))

	return n.Reply(msg, body)
}

// topology_handler
func topology_handler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	topology := body["topology"].(map[string]interface{})
	neighbours_array := topology[n.ID()].([]interface{})

	neighbours = make([]string, len(neighbours_array))
	for i, nei := range neighbours_array {
		neighbour := nei.(string)
		neighbours[i] = neighbour
		neighbours_seen[neighbour] = mapset.NewSet[int]()
	}

	response := make(map[string]any)
	response["type"] = "topology_ok"

	return n.Reply(msg, response)
}

func main() {
	messages = mapset.NewSet[int]()
	neighbours_seen = make(map[string]mapset.Set[int])

	n = maelstrom.NewNode()

	n.Handle("broadcast", broadcast_handler)
	n.Handle("read", read_handler)
	n.Handle("topology", topology_handler)
	n.Handle("node_propagate", node_propagate_handler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
