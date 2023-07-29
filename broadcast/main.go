package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var messages []int
var n *maelstrom.Node

// propagate_to_other_nodes sends the recieved new message to all other nodes.
func propagate_to_other_nodes(new_id int) error {
	response := make(map[string]any)
	response["type"] = "node_propagate"
	response["message"] = new_id

	log.Println("Node ", n.ID(), " Neighbours: ", n.NodeIDs())
	// for _, node := range n.NodeIDs() {
	// 	if node != n.ID() {
	// 		if err := n.Send(node, response); err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	for _, node := range n.NodeIDs() {
		log.Println("Node ", n.ID(), " Sending to ", node, " Message", new_id)
		if node != n.ID() {
			if err := n.RPC(node, response, func(msg maelstrom.Message) error {
				var body map[string]any
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				log.Printf("%s Send to %s successfully\n", n.ID(), msg.Src)
				if body["type"] != "node_propagate_ok" {
					log.Println("fuck error 2!")
				}
				return nil
			}); err != nil {
				log.Println("fuck error!")
			}
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

	new_id := int(body["message"].(float64))

	response := make(map[string]any)
	response["type"] = "node_propagate_ok"

	// If it has new_id already, return. Otherwise also share it with others
	for _, m := range messages {
		if m == new_id {
			return n.Reply(msg, response)
		}
	}

	messages = append(messages, new_id)
	propagate_to_other_nodes(new_id)

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
	messages = append(messages, new_id)

	if err := propagate_to_other_nodes(new_id); err != nil {
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

	topology := body["topology"].(map[string]interface{})
	neighbours := topology[n.ID()].([]interface{})

	neighbours_list := make([]string, len(neighbours))
	for i, nei := range neighbours {
		neighbours_list[i] = nei.(string)
		// neighbours_list = append(neighbours_list, nei.(string))
	}

	// neighbours := make([]string, len(topology[n.ID()]))
	// for _, neighbour := range topology[n.ID()] {
	// 	neighbours = append(neighbours, neighbour.(string))
	// }
	// log.Panicln("neighboursss ", neighbours)
	n.Init(n.ID(), neighbours_list)

	response := make(map[string]any)
	response["type"] = "topology_ok"

	return n.Reply(msg, response)
}

func main() {
	n = maelstrom.NewNode()

	n.Handle("broadcast", broadcast_handler)
	n.Handle("read", read_handler)
	n.Handle("topology", topology_handler)
	n.Handle("node_propagate", node_propagate_handler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
