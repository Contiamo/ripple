package ripple

import (
	"fmt"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	dialTo := testDialTo()
	queue := "ripple_test_queue"

	qChan := make(chan bool)
	handler := func(msg []byte, q string) error {
		qChan <- true
		fmt.Printf("msg: %s\n", msg)
		return nil
	}

	s, err := NewQueueSubscriber(queue, dialTo, handler)
	if err != nil {
		t.Fatal(err)
	}
	go s.Listen()

	pubMsg := "it worked!"
	p := NewQueuePublisher(dialTo)
	p.Pub(queue, []byte(pubMsg))

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Expected handler function to be called")
	case <-qChan:
	}
}
