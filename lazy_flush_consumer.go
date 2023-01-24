package beeorm

import (
	"context"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

const LazyFlushChannelName = "orm-lazy-flush-stream"
const LazyFlushGroupName = "orm-lazy-flush-consumer"

type LazyFlushConsumer struct {
	eventConsumerBase
	consumer                     *eventsConsumer
	lazyFlushQueryErrorResolvers []LazyFlushQueryErrorResolver
	flusher                      *flusher
}

func NewLazyFlushConsumer(engine Engine) *LazyFlushConsumer {
	c := &LazyFlushConsumer{}
	c.engine = engine.(*engineImplementation)
	c.block = true
	c.blockTime = time.Second * 30
	c.flusher = &flusher{engine: engine.(*engineImplementation)}
	return c
}

type LazyFlushQueryErrorResolver func(engine Engine, db *DB, sql string, queryError *mysql.MySQLError) error

func (r *LazyFlushConsumer) RegisterLazyFlushQueryErrorResolver(resolver LazyFlushQueryErrorResolver) {
	r.lazyFlushQueryErrorResolvers = append(r.lazyFlushQueryErrorResolvers, resolver)
}

func (r *LazyFlushConsumer) Digest(ctx context.Context) bool {
	r.consumer = r.engine.GetEventBroker().Consumer(LazyFlushGroupName).(*eventsConsumer)
	r.consumer.eventConsumerBase = r.eventConsumerBase
	return r.consumer.Consume(ctx, 500, func(events []Event) {
		lazyEvents := make([]*EntitySQLFlush, 0)
		for _, e := range events {
			switch e.Stream() {
			case LazyFlushChannelName:
				var data []*EntitySQLFlush
				e.Unserialize(&data)
				lazyEvents = append(lazyEvents, data...)
			}
		}
		r.handleEvents(events, lazyEvents)
	})
}

func (r *LazyFlushConsumer) handleEvents(events []Event, lazyEvents []*EntitySQLFlush) {
	//TODO handle errors
	defer func() {
		if rec := recover(); rec != nil {
			err, asErr := rec.(error)
			if !asErr {
				panic(rec)
			}
			fmt.Printf("ERR %v\n", err)
			for i, e := range lazyEvents {
				f := &flusher{engine: r.engine}
				f.events = []*EntitySQLFlush{e}
				func() {
					defer func() {
						if rec2 := recover(); rec2 != nil {
							err2, asErr2 := rec.(error)
							if !asErr2 {
								panic(rec2)
							}
							errSQL, asErrSQL := err2.(SqlE)
							for _, errorResolver := range r.lazyFlushQueryErrorResolvers {

							}
							fmt.Printf("ERR2 %v\n", err2)
							return
						}
						events[i].Ack()
					}()
					f.execute(false)
				}()
			}
		}
	}()
	f := &flusher{engine: r.engine}
	f.events = lazyEvents
	f.execute(false)
}
