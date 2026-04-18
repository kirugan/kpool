package kpool_test

import (
	"fmt"
	"time"

	"github.com/kirugan/kpool"
)

type Message struct {
	UserID string
	Body   string
}

func Example() {
	p, err := kpool.New[string, Message](10, 128, func(msg Message) {
		fmt.Printf("start user=%s body=%s at %s\n", msg.UserID, msg.Body, time.Now().Format("15:04:05.000"))
		time.Sleep(500 * time.Millisecond)
		fmt.Printf("done  user=%s body=%s at %s\n", msg.UserID, msg.Body, time.Now().Format("15:04:05.000"))
	})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	p.Submit("u1", Message{UserID: "u1", Body: "a"})
	p.Submit("u2", Message{UserID: "u2", Body: "b"})
	p.Submit("u1", Message{UserID: "u1", Body: "c"})
	p.Submit("u3", Message{UserID: "u3", Body: "d"})
	p.Submit("u2", Message{UserID: "u2", Body: "e"})

	time.Sleep(3 * time.Second)
}
