package beeorm

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

type QueryLoggerSource int

const sourceMySQL = "mysql"
const sourceRedis = "redis"
const sourceLocalCache = "local_cache"
const beeORMLogo = "\u001B[1m\x1b[38;2;0;0;0;48;2;255;255;255mBee\u001B[38;2;254;147;51mORM \u001B[0m\x1b[0m\u001B[0m"
const mysqlLogo = "\x1b[38;2;2;117;143;48;2;255;255;255mMy\u001B[38;2;242;145;17mSQL \u001B[0m\x1b[0m\u001B[0m"
const redisLogo = "\u001B[1m\x1b[38;2;191;56;42;48;2;255;255;255mredis \u001B[0m\x1b[0m\u001B[0m"
const localCacheLogo = "\u001B[1m\x1b[38;2;254;147;51;48;2;255;255;255mlocal \u001B[0m\x1b[0m\u001B[0m"
const timeTemplate = "\x1b[38;2;0;0;0;48;2;255;%d;%dm %0.1fms%s \u001B[0m\x1b[0m\u001B[0m\n"
const operationTemplate = "\u001B[1m\x1b[38;2;0;0;0;48;2;255;255;255m%-14s\u001B[0m\x1b[0m\u001B[0m"
const queryTemplate = "\x1b[38;2;255;255;155m%s\u001B[0m\x1b[0m\u001B[0m\n"
const errorTemplate = "\x1b[38;2;191;46;42m%s\u001B[0m\x1b[0m\u001B[0m\n"

type defaultLogLogger struct {
	maxPoolLen int
	logger     *log.Logger
}

func (d *defaultLogLogger) Handle(_ Context, fields map[string]interface{}) {
	row := beeORMLogo
	switch fields["source"] {
	case "mysql":
		row += mysqlLogo
	case "redis":
		row += redisLogo
	case "local_cache":
		row += localCacheLogo
	}
	poolTemplate := "\u001B[1m\x1b[38;2;175;175;175;48;2;255;255;255m%-" + strconv.Itoa(d.maxPoolLen) + "s\u001B[0m\x1b[0m\u001B[0m"
	row += fmt.Sprintf(poolTemplate, fields["pool"])
	microseconds := float64(0)
	microsecondsVal, has := fields["microseconds"]
	timeSuffix := ""
	timeBackground := 255
	if has {
		microseconds = float64(microsecondsVal.(int64))
		timeBackground -= int(microseconds / 2400)
		timeBackground = int(math.Max(float64(0), float64(timeBackground)))
		timeSuffix = strings.Repeat(" ", int(microseconds/10000))
	}
	seconds := microseconds / 1000
	row += fmt.Sprintf(operationTemplate, fields["operation"])
	row += fmt.Sprintf(timeTemplate, timeBackground, timeBackground, seconds, timeSuffix)
	row += fmt.Sprintf(queryTemplate, fields["query"])
	err, hasError := fields["error"]
	if hasError {
		row += fmt.Sprintf(errorTemplate, err)
	}
	d.logger.Print(row)
}

type LogHandler interface {
	Handle(c Context, log map[string]interface{})
}

func (c *contextImplementation) RegisterQueryLogger(handler LogHandler, mysql, redis, local bool) {
	if mysql {
		c.hasDBLogger = true
		c.queryLoggersDB = c.appendLog(c.queryLoggersDB, handler)
	}
	if redis {
		c.hasRedisLogger = true
		c.queryLoggersRedis = c.appendLog(c.queryLoggersRedis, handler)
	}
	if local {
		c.hasLocalCacheLogger = true
		c.queryLoggersLocalCache = c.appendLog(c.queryLoggersLocalCache, handler)
	}
}

func (c *contextImplementation) EnableQueryDebug() {
	c.EnableQueryDebugCustom(true, true, true)
}

func (c *contextImplementation) EnableQueryDebugCustom(mysql, redis, local bool) {
	c.RegisterQueryLogger(c.engine.defaultQueryLogger, mysql, redis, local)
}

func getNow(has bool) *time.Time {
	if !has {
		return nil
	}
	s := time.Now()
	return &s
}

func (c *contextImplementation) appendLog(logs []LogHandler, toAdd LogHandler) []LogHandler {
	for _, v := range logs {
		if v == toAdd {
			return logs
		}
	}
	return append(logs, toAdd)
}

func fillLogFields(context Context, handlers []LogHandler, pool, source, operation, query string, start *time.Time, cacheMiss bool, err error) {
	fields := map[string]interface{}{
		"operation": operation,
		"query":     query,
		"pool":      pool,
		"source":    source,
	}
	if cacheMiss {
		fields["miss"] = "TRUE"
	}
	meta := context.GetMetaData()
	if len(meta) > 0 {
		fields["meta"] = meta
	}
	if start != nil {
		now := time.Now()
		fields["microseconds"] = time.Since(*start).Microseconds()
		fields["started"] = start.UnixNano()
		fields["finished"] = now.UnixNano()
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	for _, handler := range handlers {
		handler.Handle(context, fields)
	}
}
