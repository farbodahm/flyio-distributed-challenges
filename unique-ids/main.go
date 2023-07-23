package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		log.Println("New Message:", msg)

		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		id := uuid.New()
		body["type"] = "generate_ok"
		body["id"] = id.String()

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)

	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
