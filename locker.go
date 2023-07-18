package beeorm

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/redislock"

	"github.com/pkg/errors"
)

type lockerClient interface {
	Obtain(ctx context.Context, key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error)
}

type standardLockerClient struct {
	client *redislock.Client
}

func (l *standardLockerClient) Obtain(ctx context.Context, key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error) {
	return l.client.Obtain(ctx, key, ttl, opt)
}

type Locker struct {
	locker lockerClient
	r      *redisCache
}

func (r *redisCache) GetLocker() *Locker {
	if r.locker == nil {
		r.locker = &Locker{locker: &standardLockerClient{client: redislock.New(r.client)}, r: r}
	}
	return r.locker
}

func (l *Locker) Obtain(c Context, key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
	key = l.r.addNamespacePrefix(key)
	if ttl == 0 {
		panic(errors.New("ttl must be higher than zero"))
	}
	if waitTimeout > ttl {
		panic(errors.New("waitTimeout can't be higher than ttl"))
	}
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	var options *redislock.Options
	if waitTimeout > 0 {
		options = &redislock.Options{}
		interval := time.Second
		limit := 1
		if waitTimeout < interval {
			interval = waitTimeout
		} else {
			limit = int(waitTimeout / time.Second)
		}
		options.RetryStrategy = redislock.LimitRetry(redislock.LinearBackoff(interval), limit)
	}
	redisLock, err := l.locker.Obtain(c.Ctx(), key, ttl, options)
	if err != nil {
		if err == redislock.ErrNotObtained {
			if hasLogger {
				message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
				l.fillLogFields(c, "LOCK OBTAIN", message, start, true, nil)
			}
			return nil, false
		}
	}
	if hasLogger {
		message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
		l.fillLogFields(c, "LOCK OBTAIN", message, start, false, nil)
	}
	checkError(err)
	lock = &Lock{lock: redisLock, locker: l, ttl: ttl, key: key, has: true}
	return lock, true
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	ttl    time.Duration
	locker *Locker
	has    bool
}

func (l *Lock) Release(c Context) {
	if !l.has {
		return
	}
	l.has = false
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	err := l.lock.Release(context.Background())
	ok := true
	if err == redislock.ErrLockNotHeld {
		err = nil
		ok = false
	}
	if hasLogger {
		l.locker.fillLogFields(c, "LOCK RELEASE", "LOCK RELEASE "+l.key, start, !ok, err)
	}
	checkError(err)
}

func (l *Lock) TTL(c Context) time.Duration {
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	t, err := l.lock.TTL(c.Ctx())
	if hasLogger {
		l.locker.fillLogFields(c, "LOCK TTL", "LOCK TTL "+l.key, start, false, err)
	}
	checkError(err)
	return t
}

func (l *Lock) Refresh(c Context, ttl time.Duration) bool {
	if !l.has {
		return false
	}
	hasLogger, _ := c.getRedisLoggers()
	start := getNow(hasLogger)
	err := l.lock.Refresh(c.Ctx(), ttl, nil)
	ok := true
	if err == redislock.ErrNotObtained {
		ok = false
		err = nil
		l.has = false
	}
	if hasLogger {
		message := fmt.Sprintf("LOCK REFRESH %s %s", l.key, l.ttl)
		l.locker.fillLogFields(c, "LOCK REFRESH", message, start, !ok, err)
	}
	checkError(err)
	return ok
}

func (l *Locker) fillLogFields(c Context, operation, query string, start *time.Time, cacheMiss bool, err error) {
	_, loggers := c.getRedisLoggers()
	fillLogFields(c, loggers, l.r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
