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

	stop chan struct{}
}

func NewQueueSubscriber(queue, dialTo string, h HandlerFunc) (*QueueSubscriber, error) {
	c, err := dial(dialTo)
	if err != nil {
		return nil, err
	}

	stop := make(chan struct{})

	return &QueueSubscriber{queue: queue, conn: c, handler: h, stop: stop}, nil
}

// Starts a goroutine to listen for messages.
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
					log.Printf("ripple_queue: Error receiving msg: %s", res.err)
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

func (s *QueueSubscriber) Stop() {
	s.stop <- struct{}{}
}

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
