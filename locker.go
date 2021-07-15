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
	engine *Engine
	code   string
}

func (r *RedisCache) GetLocker() *Locker {
	if r.locker != nil {
		return r.locker
	}
	lockerClient := &standardLockerClient{client: redislock.New(r.client)}
	r.locker = &Locker{locker: lockerClient, engine: r.engine, code: r.config.GetCode()}
	return r.locker
}

func (l *Locker) Obtain(key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
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
	start := getNow(l.engine.hasRedisLogger)
	redisLock, err := l.locker.Obtain(l.engine.context, key, ttl, options)
	if err != nil {
		if err == redislock.ErrNotObtained {
			if l.engine.hasRedisLogger {
				message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
				l.fillLogFields("LOCK OBTAIN", message, start, nil)
			}
			return nil, false
		}
	}
	if l.engine.hasRedisLogger {
		message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
		l.fillLogFields("LOCK OBTAIN", message, start, nil)
	}
	checkError(err)
	lock = &Lock{lock: redisLock, locker: l, key: key, has: true, engine: l.engine}
	lock.timer = time.NewTimer(ttl)
	lock.done = make(chan bool)
	go func() {
		for {
			select {
			case <-l.engine.context.Done():
				lock.Release()
				return
			case <-lock.timer.C:
				return
			case <-lock.done:
				return
			}
		}
	}()
	return lock, true
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	locker *Locker
	has    bool
	engine *Engine
	timer  *time.Timer
	done   chan bool
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
		l.locker.fillLogFields("LOCK RELEASE", "LOCK RELEASE "+l.key, start, err)
	}
	checkError(err)
	close(l.done)
}

func (l *Lock) TTL() time.Duration {
	start := getNow(l.engine.hasRedisLogger)
	d, err := l.lock.TTL(l.engine.context)
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("LOCK TTL", "LOCK TTL "+l.key, start, err)
	}
	checkError(err)
	return d
}

func (l *Lock) Refresh(ttl time.Duration) bool {
	if !l.has {
		return false
	}
	start := getNow(l.engine.hasRedisLogger)
	err := l.lock.Refresh(l.engine.context, ttl, nil)
	has := true
	if err == redislock.ErrNotObtained {
		has = false
		err = nil
		l.has = false
	}
	l.timer.Reset(ttl)
	if l.engine.hasRedisLogger {
		message := fmt.Sprintf("LOCK REFRESH %s %s", l.key, ttl.String())
		l.locker.fillLogFields("LOCK REFRESH", message, start, err)
	}
	checkError(err)
	return has
}

func (l *Locker) fillLogFields(operation, query string, start *time.Time, err error) {
	fillLogFields(l.engine.queryLoggersRedis, l.code, sourceRedis, operation, query, start, err)
}
