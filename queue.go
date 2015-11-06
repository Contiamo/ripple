package ripple

import (
	"encoding/base64"
	"log"

	"github.com/garyburd/redigo/redis"
)

type QueuePublisher struct {
	connPool *redis.Pool
	dialTo   string
}

func NewQueuePublisher(dialTo string) *QueuePublisher {
	connPool := redisPool(dialTo)
	return &QueuePublisher{connPool: connPool, dialTo: dialTo}
}

const QueuePubCmd = "LPUSH"

func (p *QueuePublisher) Pub(queue string, b []byte) error {
	conn := p.connPool.Get()
	defer conn.Close()

	// base64 encode msg
	msg := base64.StdEncoding.EncodeToString(b)

	_, err := conn.Do(QueuePubCmd, queue, msg)
	return err
}

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

type QueueSubscriber struct {
	conn    redis.Conn
	queue   string
	handler HandlerFunc
}

func NewQueueSubscriber(queue, dialTo string, h HandlerFunc) (*QueueSubscriber, error) {
	c, err := dial(dialTo)
	if err != nil {
		return nil, err
	}

	return &QueueSubscriber{queue: queue, conn: c, handler: h}, nil
}

// Starts a goroutine to listen for messages.
func (s *QueueSubscriber) Listen() {
	// start listening
	for {
		err := s.ListenOnce()
		if err != nil {
			log.Printf("ripple_queue: Error handling msg: %s", err)
		}
	}
}

const QueueSubCmd = "BRPOP"

func (s *QueueSubscriber) ListenOnce() error {
	reply, err := redis.Values(s.conn.Do(QueueSubCmd, s.queue, 0))
	if err != nil {
		panic(err)
	}

	var qName string
	var msg string
	if _, err := redis.Scan(reply, &qName, &msg); err != nil {
		return err
	}

	// base64 decode msg
	b, err := base64.StdEncoding.DecodeString(msg)
	if err != nil {
		return err
	}

	return s.handler(b, s.queue)
}
