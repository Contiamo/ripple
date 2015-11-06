package ripple

import (
	"testing"
	"time"
)

func TestListenOnce(t *testing.T) {
	dialTo := testDialTo()

	pub := NewPublisher(dialTo)
	sub := NewSubscriber(dialTo)

	redisCh := "ripple_test"
	callback := make(chan []byte, 1)
	sub.On(redisCh, func(msg []byte, ch string) error {
		callback <- msg
		return nil
	})

	err := sub.ListenOnce(func() {
		pub.Pub(redisCh, []byte("just rippling through"))
	}, time.Second)

	if err != nil {
		t.Fatal(err)
	}

	msg := <-callback
	if string(msg) != "just rippling through" {
		t.Errorf(`Unexpected ripple message: "%s"`, msg)
	}
}
