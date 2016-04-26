package ripple

import (
	"encoding/base64"
	"fmt"
	"log"

	"github.com/garyburd/redigo/redis"
)

// QueuePublisher publishes a message to a list.
type QueuePublisher struct {
	connPool *redis.Pool
	dialTo   string
}

// NewQueuePublisher returns a queue publisher ready to insert keys.
func NewQueuePublisher(dialTo string) *QueuePublisher {
	connPool := redisPool(dialTo)
	return &QueuePublisher{connPool: connPool, dialTo: dialTo}
}

// Command to queue a message.
const QueuePubCmd = "LPUSH"

// Pub inserts a message into list queue.
func (p *QueuePublisher) Pub(queue string, b []byte) error {
	conn := p.connPool.Get()
	defer conn.Close()

	// base64 encode msg
	msg := base64.StdEncoding.EncodeToString(b)

	_, err := conn.Do(QueuePubCmd, queue, msg)
	return err
}

// MultiPub inserts a slice of messages into list queue.
func (p *QueuePublisher) MultiPub(queue string, bs [][]byte) error {
	conn := p.connPool.Get()
	defer conn.Close()

	args := make([]interface{}, len(bs)+1)
	args[0] = queue

	for i, b := range bs {
		// base64 encode msg
		args[i+1] = base64.StdEncoding.EncodeToString(b)
	}

	_, err := conn.Do(QueuePubCmd, args...)
	return err
}

// QueueSubscriber subscribes to a list.
type QueueSubscriber struct {
	conn    redis.Conn
	queue   string
	handler HandlerFunc

	dialTo string
	stop   chan struct{}
}

// NewQueueSubscriber returns a queue subscriber ready to subscribe.
func NewQueueSubscriber(queue, dialTo string, h HandlerFunc) (*QueueSubscriber, error) {
	c, err := dial(dialTo)
	if err != nil {
		return nil, err
	}

	stop := make(chan struct{})

	return &QueueSubscriber{queue: queue, conn: c, handler: h, dialTo: dialTo, stop: stop}, nil
}

// Listen starts a goroutine to listen for messages.
func (s *QueueSubscriber) Listen() {
	for {
		select {
		case <-s.stop:
			return

		default:
			select {
			case <-s.stop:
				return

			case res := <-s.pop():
				if res.err != nil {
					if !s.reconnect() {
						// since the connection is fine it's probably ok to log here
						// - don't want to log in a tight loop which fails every time (i.e. when the connection is down)
						log.Printf("ripple_queue: Error popping message: %s", res.err)
					}
				} else {
					// execute handler
					err := s.handler(res.b, s.queue)
					if err != nil {
						log.Printf("ripple_queue: Error handling msg: %s", err)
					}
				}
			}
		}
	}
}

// Stop closes the open connection.
func (s *QueueSubscriber) Stop() {
	s.stop <- struct{}{}
	s.conn.Close()
}

// reconnect checks if the connection valid, otherwise dials a new one
func (s *QueueSubscriber) reconnect() bool {
	_, err := s.conn.Do("PING")
	if err == nil {
		// connection is already active - no reconnect needed
		return false
	}

	conn, err := dialWithRetry(s.dialTo)
	if err != nil {
		panic(fmt.Sprintf("ripple_queue subscriber dial failed: %s", err))
	}

	s.conn = conn
	return true
}

// Command to retrieve a message from the queue.
const QueueSubCmd = "BRPOP"

type response struct {
	b   []byte
	err error
}

func (s *QueueSubscriber) pop() chan response {
	ch := make(chan response)

	go s.redisPop(ch)

	return ch
}

func (s *QueueSubscriber) redisPop(ch chan response) {
	var res response

	reply, err := redis.Values(s.conn.Do(QueueSubCmd, s.queue, 0))
	if err != nil {
		res.err = err
		ch <- res
		return
	}

	var qName string
	var msg string
	if _, err := redis.Scan(reply, &qName, &msg); err != nil {
		res.err = err
		ch <- res
		return
	}

	// base64 decode msg
	b, err := base64.StdEncoding.DecodeString(msg)
	if err != nil {
		res.err = err
		ch <- res
		return
	}

	res.b = b
	ch <- res
}
