package ripple

import (
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
)

// HandlerFunc handles a queue message.
type HandlerFunc func(msg []byte, queue string) error

func redisPool(dialTo string) *redis.Pool {
	maxIdle := 50
	idleTimeout := 300

	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: time.Duration(idleTimeout) * time.Second,
		Dial: func() (redis.Conn, error) {
			return dialWithRetry(dialTo)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

var dialRetryWaits = []time.Duration{1, 2, 4, 8, 30}
var dialDefaultWait = time.Duration(60)

const dialMaxRetries = 15

// dialWithRetry attempts multiple times to connect to redis if not successful.
//
// Attempts will be made to reconnect for ~ 650s after which an error will be returned.
func dialWithRetry(dialTo string) (redis.Conn, error) {
	if conn, err := dial(dialTo); err == nil {
		return conn, nil
	}

	for i := 0; i < dialMaxRetries; i++ {
		var timeout time.Duration
		if i < len(dialRetryWaits) {
			timeout = dialRetryWaits[i]
		} else {
			timeout = dialDefaultWait
		}

		<-time.After(timeout * time.Second)

		log.Println("ripple dialWithRetry attempting to reconnect to redis")

		// redial
		if conn, err := dial(dialTo); err == nil {
			return conn, nil
		}

	}

	return nil, fmt.Errorf("ripple dialWithRetry could not connect to redis")
}

func dial(dialTo string) (redis.Conn, error) {
	return redis.Dial("tcp", dialTo)
}
