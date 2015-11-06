package ripple

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type HandlerFunc func([]byte, string) error

func redisPool(dialTo string) *redis.Pool {
	maxIdle := 50
	idleTimeout := 300

	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: time.Duration(idleTimeout) * time.Second,
		Dial: func() (redis.Conn, error) {
			return dial(dialTo)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			// _, err := c.Do("PING")
			// return err
			// do not ping for every connection
			return nil
		},
	}
}

func dial(dialTo string) (redis.Conn, error) {
	return redis.Dial("tcp", dialTo)
}
