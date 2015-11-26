package ripple

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Publisher publishes a message to a PUBSUB channel.
type Publisher struct {
	connPool *redis.Pool
	dialTo   string
}

// NewPublisher returns a publisher ready to publish.
func NewPublisher(dialTo string) *Publisher {
	connPool := redisPool(dialTo)
	return &Publisher{connPool: connPool, dialTo: dialTo}
}

// Pub publishes a message to a Redis channel.
func (p *Publisher) Pub(channel string, b []byte) error {
	conn := p.connPool.Get()
	defer conn.Close()

	// base64 encode msg
	msg := base64.StdEncoding.EncodeToString(b)

	_, err := conn.Do("PUBLISH", channel, msg)
	return err
}

// Stop stops the publisher.
func (p *Publisher) Stop() error {
	return p.connPool.Close()
}

// Subscriber subscribes to a PUBSUB channel.
type Subscriber struct {
	dialTo        string
	listenOn      redis.PubSubConn
	subscriptions map[string]HandlerFunc
	subPatterns   map[string]HandlerFunc
	stop          chan struct{}
}

// NewSubscriber returns a subscriber ready to subscribe.
func NewSubscriber(dialTo string) *Subscriber {
	s := &Subscriber{dialTo: dialTo}
	s.subscriptions = make(map[string]HandlerFunc)
	s.subPatterns = make(map[string]HandlerFunc)
	s.stop = make(chan struct{})
	return s
}

// On specifies a handler function for a given PUBSUB channel subscription.
func (s *Subscriber) On(channel string, f HandlerFunc) {
	s.subscriptions[channel] = f
}

// OnPattern specifies a handler function for a given PUBSUB channel subscribe
// pattern.
func (s *Subscriber) OnPattern(chanPattern string, f HandlerFunc) {
	s.subPatterns[chanPattern] = f
}

// Stop closes the open connection.
func (s *Subscriber) Stop() error {
	s.stop <- struct{}{}

	if err := s.stopListen(); err != nil {
		return err
	}

	return s.listenOn.Close()
}

// Listen is a blocking loop that waits for and handles messages.
func (s *Subscriber) Listen() error {
	if err := s.setupListen(); err != nil {
		return err
	}

	select {
	case <-s.stop:
		return nil

	case err := <-s.receive():
		return err
	}
}

// ListenOnce blocks and then exits once the first message is received.
func (s *Subscriber) ListenOnce(f func(), timeout time.Duration) error {
	if err := s.setupListen(); err != nil {
		return err
	}

	f()

	select {
	case <-time.After(timeout):
		return fmt.Errorf("ripple: ListenOnce timeout - no message received")

	case <-s.stop:
		return nil

	case err := <-s.receive():
		return err
	}
}

func (s *Subscriber) setupListen() error {
	c, err := dial(s.dialTo)
	if err != nil {
		return err
	}

	s.listenOn = redis.PubSubConn{Conn: c}

	// subscribe
	for ch := range s.subscriptions {
		if err := s.listenOn.Subscribe(ch); err != nil {
			return fmt.Errorf("ripple: error subscribing to channel %q (%v)", ch, err)
		}
	}

	// subscribe to patterns
	for pat := range s.subPatterns {
		if err := s.listenOn.PSubscribe(pat); err != nil {
			return fmt.Errorf("ripple: error subscribing to channel pattern %q (%v)", pat, err)
		}
	}

	return nil
}

func (s *Subscriber) stopListen() error {
	// unsubscribe
	for ch := range s.subscriptions {
		err := s.listenOn.Unsubscribe(ch)
		if err != nil {
			return fmt.Errorf("ripple: error unsubscribing to channel %q (%v)", ch, err)
		}
	}

	// unsubscribe to patterns
	for pat := range s.subPatterns {
		err := s.listenOn.PUnsubscribe(pat)
		if err != nil {
			return fmt.Errorf("ripple: error unsubscribing to channel pattern %q (%v)", pat, err)
		}
	}

	return nil
}

func (s *Subscriber) receive() <-chan error {
	errch := make(chan error)

	go func() {
		for {
		RECEIVE:
			switch v := s.listenOn.Receive().(type) {
			default:
				// ignore this message type and try again
				goto RECEIVE
			case redis.Message:
				msg := string(v.Data)
				b, err := base64.StdEncoding.DecodeString(msg)
				if err != nil {
					errch <- fmt.Errorf("ripple:Could not decode msg: %s", msg)
					return
				}
				channel := v.Channel

				if f, ok := s.subscriptions[channel]; ok {
					errch <- f(b, channel)
					return
				}
				errch <- fmt.Errorf("handler not found in subscriptions")
				return
			case redis.PMessage:
				msg := string(v.Data)
				b, err := base64.StdEncoding.DecodeString(msg)
				if err != nil {
					errch <- fmt.Errorf("ripple:Could not decode msg: %s", msg)
					return
				}
				channel := v.Channel
				pattern := v.Pattern

				if f, ok := s.subPatterns[pattern]; ok {
					errch <- f(b, channel)
					return
				}
				errch <- fmt.Errorf("handler not found in subscription patterns")
			case error:
				errch <- v
				return
			}
		}
	}()

	return errch
}
