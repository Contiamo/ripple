package ripple

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Publisher struct {
	connPool *redis.Pool
	dialTo   string
}

func NewPublisher(dialTo string) *Publisher {
	connPool := redisPool(dialTo)
	return &Publisher{connPool: connPool, dialTo: dialTo}
}

func (p *Publisher) Pub(channel string, b []byte) error {
	conn := p.connPool.Get()
	defer conn.Close()

	// base64 encode msg
	msg := base64.StdEncoding.EncodeToString(b)

	_, err := conn.Do("PUBLISH", channel, msg)
	return err
}

type Subscriber struct {
	dialTo        string
	listenOn      redis.PubSubConn
	subscriptions map[string]HandlerFunc
	subPatterns   map[string]HandlerFunc
}

func NewSubscriber(dialTo string) *Subscriber {
	s := &Subscriber{dialTo: dialTo}
	s.subscriptions = make(map[string]HandlerFunc)
	s.subPatterns = make(map[string]HandlerFunc)
	return s
}

func (s *Subscriber) On(channel string, f HandlerFunc) {
	s.subscriptions[channel] = f
}

// subscribe to a pattern
func (s *Subscriber) OnPattern(chanPattern string, f HandlerFunc) {
	s.subPatterns[chanPattern] = f
}

// Starts a goroutine to listen for messages.
func (s *Subscriber) Listen() {
	s.setupListen()

	// start listening
	for {
		s.receive()
	}
}

// Blocking listen - exits once the first message is received.
func (s *Subscriber) ListenOnce(f func(), timeout time.Duration) error {
	s.setupListen()
	f()

	res := make(chan error)
	go func() {
		err := s.receive()
		res <- err
	}()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("ripple: ListenOnce timeout - no message received")

	case err := <-res:
		return err
	}
}

func (s *Subscriber) setupListen() {
	c, err := dial(s.dialTo)
	if err != nil {
		panic(err)
	}

	s.listenOn = redis.PubSubConn{c}

	// subscribe
	for channel, _ := range s.subscriptions {
		s.listenOn.Subscribe(channel)
	}

	// subscribe to patterns
	for chanPattern, _ := range s.subPatterns {
		s.listenOn.PSubscribe(chanPattern)
	}
}

func (s *Subscriber) receive() error {
RECEIVE:
	switch v := s.listenOn.Receive().(type) {
	default:
		// ignore this message type and try again
		goto RECEIVE
	case redis.Message:
		msg := string(v.Data)
		b, err := base64.StdEncoding.DecodeString(msg)
		if err != nil {
			return fmt.Errorf("ripple:Could not decode msg: %s", msg)
		}
		channel := v.Channel

		if f, ok := s.subscriptions[channel]; ok {
			return f(b, channel)
		} else {
			return fmt.Errorf("handler not found in subscriptions")
		}
	case redis.PMessage:
		msg := string(v.Data)
		b, err := base64.StdEncoding.DecodeString(msg)
		if err != nil {
			return fmt.Errorf("ripple:Could not decode msg: %s", msg)
		}
		channel := v.Channel
		pattern := v.Pattern

		if f, ok := s.subPatterns[pattern]; ok {
			return f(b, channel)
		} else {
			return fmt.Errorf("handler not found in subscription patterns")
		}
	case error:
		return v
	}
}
