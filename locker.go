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

func (l *Locker) Obtain(ctx context.Context, key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
	key = l.r.addNamespacePrefix(key)
	if ttl == 0 {
		panic(errors.New("ttl must be higher than zero"))
	}
	if waitTimeout > ttl {
		panic(errors.New("waitTimeout can't be higher than ttl"))
	}
	start := getNow(l.r.engine.hasRedisLogger)
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
	redisLock, err := l.locker.Obtain(ctx, key, ttl, options)
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
	lock = &Lock{lock: redisLock, locker: l, ttl: ttl, key: key, has: true, engine: l.r.engine}
	return lock, true
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	ttl    time.Duration
	locker *Locker
	has    bool
	engine *engineImplementation
}

func (l *Lock) Release() {
	if !l.has {
		return
	}
	l.has = false
	start := getNow(l.engine.hasRedisLogger)
	err := l.lock.Release(context.Background())
	ok := true
	if err == redislock.ErrLockNotHeld {
		err = nil
		ok = false
	}
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("LOCK RELEASE", "LOCK RELEASE "+l.key, start, !ok, err)
	}
	checkError(err)
}

func (l *Lock) TTL(ctx context.Context) time.Duration {
	start := getNow(l.engine.hasRedisLogger)
	t, err := l.lock.TTL(ctx)
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("LOCK TTL", "LOCK TTL "+l.key, start, false, err)
	}
	checkError(err)
	return t
}

func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) bool {
	if !l.has {
		return false
	}
	start := getNow(l.engine.hasRedisLogger)
	err := l.lock.Refresh(ctx, ttl, nil)
	ok := true
	if err == redislock.ErrNotObtained {
		ok = false
		err = nil
		l.has = false
	}
	if l.engine.hasRedisLogger {
		message := fmt.Sprintf("LOCK REFRESH %s %s", l.key, l.ttl)
		l.locker.fillLogFields("LOCK REFRESH", message, start, !ok, err)
	}
	checkError(err)
	return ok
}

func (l *Locker) fillLogFields(operation, query string, start *time.Time, cacheMiss bool, err error) {
	fillLogFields(l.r.engine.queryLoggersRedis, l.r.config.GetCode(), sourceRedis, operation, query, start, cacheMiss, err)
}
