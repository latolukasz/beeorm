package beeorm

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/pkg/errors"
)

type lockerClient interface {
	Obtain(ctx context.Context, key string, options ...redsync.Option) (*redsync.Mutex, error)
}

type standardLockerClient struct {
	client *redsync.Redsync
}

func (l *standardLockerClient) Obtain(ctx context.Context, key string, options ...redsync.Option) (*redsync.Mutex, error) {
	mutex := l.client.NewMutex(key, options...)
	return mutex, mutex.LockContext(ctx)

}

type Locker struct {
	locker lockerClient
	r      *RedisCache
}

func (r *RedisCache) GetLocker() *Locker {
	if r.locker != nil {
		return r.locker
	}
	client := r.client
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)
	lockerClient := &standardLockerClient{client: rs}
	r.locker = &Locker{locker: lockerClient, r: r}
	return r.locker
}

func (l *Locker) Obtain(ctx context.Context, key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
	key = l.r.addNamespacePrefix(key)
	if ttl == 0 {
		panic(errors.New("ttl must be higher than zero"))
	}
	start := getNow(l.r.engine.hasRedisLogger)
	var mutex *redsync.Mutex
	var err error
	if waitTimeout == 0 {
		mutex, err = l.locker.Obtain(ctx, key, redsync.WithExpiry(ttl), redsync.WithTries(1))
	} else {
		minDelay := 50 * time.Millisecond
		tries := 10
		delay := time.Duration(waitTimeout.Nanoseconds() / int64(tries))
		if delay < minDelay {
			delay = minDelay
			tries = int(waitTimeout.Nanoseconds()/minDelay.Nanoseconds()) + 1
		}
		mutex, err = l.locker.Obtain(ctx, key, redsync.WithExpiry(ttl), redsync.WithTries(tries), redsync.WithRetryDelay(delay))
	}
	if err != nil {
		if err == redsync.ErrFailed {
			if l.r.engine.hasRedisLogger {
				message := fmt.Sprintf("LOCK OBTAIN %s TTL %s WAIT %s", key, ttl.String(), waitTimeout.String())
				l.fillLogFields("LOCK OBTAIN", message, start, true, nil)
			}
			return nil, false
		}
		multiError, is := err.(*multierror.Error)
		if is && multiError.Errors[0].Error() == "context deadline exceeded" {
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
	lock = &Lock{lock: mutex, locker: l, ttl: ttl, key: key, has: true, engine: l.r.engine}
	return lock, true
}

type Lock struct {
	lock   *redsync.Mutex
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
	ok, err := l.lock.UnlockContext(context.Background())
	multiError, is := err.(*multierror.Error)
	if is && (multiError.Errors[0].Error() == "context canceled" || multiError.Errors[0].Error() == "context deadline exceeded") {
		err = nil
	}
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("LOCK RELEASE", "LOCK RELEASE "+l.key, start, !ok, err)
	}
	checkError(err)
}

func (l *Lock) TTL() time.Duration {
	start := getNow(l.engine.hasRedisLogger)
	t := l.lock.Until()
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("LOCK TTL", "LOCK TTL "+l.key, start, false, nil)
	}
	return t.Sub(time.Now())
}

func (l *Lock) Refresh(ctx context.Context) bool {
	if !l.has {
		return false
	}
	start := getNow(l.engine.hasRedisLogger)
	ok, err := l.lock.ExtendContext(ctx)
	if err != nil {
		if err == redsync.ErrExtendFailed {
			ok = false
			err = nil
			l.has = false
		} else {
			multiError, is := err.(*multierror.Error)
			if is && (multiError.Errors[0].Error() == "context canceled" || multiError.Errors[0].Error() == "context deadline exceeded") {
				ok = false
				err = nil
				l.has = false
			}
		}
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
