package beeorm

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/bsm/redislock"
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
	r      *RedisCache
}

func (r *RedisCache) GetLocker() *Locker {
	if r.locker != nil {
		return r.locker
	}
	lockerClient := &standardLockerClient{client: redislock.New(r.client)}
	r.locker = &Locker{locker: lockerClient, r: r}
	return r.locker
}

func (l *Locker) Obtain(key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
	key = l.r.addNamespacePrefix(key)
	if ttl == 0 {
		panic(errors.New("ttl must be higher than zero"))
	}
	var options *redislock.Options
	if waitTimeout > 0 {
		minInterval := 16 * time.Millisecond
		maxInterval := 256 * time.Millisecond
		max := int(waitTimeout / maxInterval)
		if max == 0 {
			max = 1
		}
		options = &redislock.Options{RetryStrategy: redislock.LimitRetry(redislock.ExponentialBackoff(minInterval, maxInterval), max)}
	}
	start := getNow(l.r.engine.hasRedisLogger)
	redisLock, err := l.locker.Obtain(context.Background(), key, ttl, options)
	if err != nil {
		if err == redislock.ErrNotObtained {
			if l.r.engine.hasRedisLogger {
				message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
				l.fillLogFields("LOCK OBTAIN", message, start, true, nil)
			}
			return nil, false
		}
	}
	if l.r.engine.hasRedisLogger {
		message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
		l.fillLogFields("LOCK OBTAIN", message, start, false, nil)
	}
	checkError(err)
	lock = &Lock{lock: redisLock, locker: l, key: key, has: true, engine: l.r.engine}
	return lock, true
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	locker *Locker
	has    bool
	engine *Engine
}

func (l *Lock) Release() {
	if !l.has {
		return
	}
	l.has = false
	start := getNow(l.engine.hasRedisLogger)
	err := l.lock.Release(context.Background())
	if err == redislock.ErrLockNotHeld {
		err = nil
	}
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("LOCK RELEASE", "LOCK RELEASE "+l.key, start, false, err)
	}
	checkError(err)
}

func (l *Lock) TTL() time.Duration {
	start := getNow(l.engine.hasRedisLogger)
	d, err := l.lock.TTL(context.Background())
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("LOCK TTL", "LOCK TTL "+l.key, start, false, err)
	}
	checkError(err)
	return d
}

func (l *Lock) Refresh(ttl time.Duration) bool {
	if !l.has {
		return false
	}
	start := getNow(l.engine.hasRedisLogger)
	err := l.lock.Refresh(context.Background(), ttl, nil)
	has := true
	if err == redislock.ErrNotObtained {
		has = false
		err = nil
		l.has = false
	}
	if l.engine.hasRedisLogger {
		message := fmt.Sprintf("LOCK REFRESH %s %s", l.key, ttl.String())
		l.locker.fillLogFields("LOCK REFRESH", message, start, false, err)
	}
	checkError(err)
	return has
}

func (l *Locker) fillLogFields(operation, query string, start *time.Time, cacheMiss bool, err error) {
	fillLogFields(l.r.engine.queryLoggersRedis, l.r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
