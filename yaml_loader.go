package beeorm

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (r *Registry) InitByYaml(yaml map[string]interface{}) {
	for key, data := range yaml {
		dataAsMap := fixYamlMap(data, "orm")
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				validateOrmMysqlURI(r, value, key)
			case "redis":
				validateRedisURI(r, value, key)
			case "sentinel":
				validateSentinel(r, value, key)
			case "mysqlEncoding":
				valAsString := validateOrmString(value, key)
				r.SetDefaultEncoding(valAsString)
			case "mysqlCollate":
				valAsString := validateOrmString(value, key)
				r.SetDefaultCollate(valAsString)
			case "local_cache":
				number := validateOrmInt(value, key)
				r.RegisterLocalCache(number, key)
			}
		}
	}
}

func validateOrmMysqlURI(registry *Registry, value interface{}, key string) {
	def := fixYamlMap(value, key)
	uri := ""
	options := MySQLPoolOptions{}
	for k, v := range def {
		switch k {
		case "uri":
			uri = validateOrmString(v, "uri")
		case "ConnMaxLifetime":
			connMaxLifetime := validateOrmInt(v, "ConnMaxLifetime")
			options.ConnMaxLifetime = time.Duration(connMaxLifetime) * time.Second
		case "MaxOpenConnections":
			options.MaxOpenConnections = validateOrmInt(v, "MaxOpenConnections")
		case "MaxIdleConnections":
			options.MaxIdleConnections = validateOrmInt(v, "MaxIdleConnections")
		}
	}
	registry.RegisterMySQLPool(uri, options, key)
}

func validateRedisURI(registry *Registry, value interface{}, key string) {
	asString, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	parts := strings.Split(asString, "?")
	elements := strings.Split(parts[0], ":")
	dbNumber := ""
	uri := ""
	isSocket := strings.Index(parts[0], ".sock") > 0
	l := len(elements)
	switch l {
	case 2:
		dbNumber = elements[1]
		uri = elements[0]
	case 3:
		if isSocket {
			dbNumber = elements[1]
			uri = elements[0]
		} else {
			dbNumber = elements[2]
			uri = elements[0] + ":" + elements[1]
		}
	case 4:
		dbNumber = elements[2]
		uri = elements[0] + ":" + elements[1]
	default:
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	db, err := strconv.ParseUint(dbNumber, 10, 64)
	if err != nil {
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	if len(parts) == 2 && parts[1] != "" {
		values, err := url.ParseQuery(parts[1])
		if err != nil {
			panic(fmt.Errorf("redis uri '%v' is not valid", value))
		}
		if values.Has("user") && values.Has("password") {
			registry.RegisterRedisWithCredentials(uri, values.Get("user"), values.Get("password"), int(db), key)
			return
		}
	}
	registry.RegisterRedis(uri, int(db), key)
}

func validateSentinel(registry *Registry, value interface{}, key string) {
	def := fixYamlMap(value, key)
	for master, values := range def {
		asSlice, ok := values.([]interface{})
		if !ok {
			panic(fmt.Errorf("sentinel '%v' is not valid", value))
		}
		asStrings := make([]string, len(asSlice))
		for i, v := range asSlice {
			asStrings[i] = fmt.Sprintf("%v", v)
		}
		db := 0
		parts := strings.Split(master, "?")
		elements := strings.Split(parts[0], ":")
		l := len(elements)
		if l >= 2 {
			master = elements[0]
			nr, err := strconv.ParseUint(elements[1], 10, 64)
			if err != nil {
				panic(fmt.Errorf("sentinel db '%v' is not valid", value))
			}
			db = int(nr)
		}
		if len(parts) == 2 && parts[1] != "" {
			values, err := url.ParseQuery(parts[1])
			if err != nil {
				panic(fmt.Errorf("sentinel uri '%v' is not valid", master))
			}
			if values.Has("user") && values.Has("password") {
				registry.RegisterRedisSentinelWithCredentials(master, values.Get("user"), values.Get("password"), db, asStrings, key)
				return
			}
		}
		registry.RegisterRedisSentinel(master, db, asStrings, key)
	}
}

func fixYamlMap(value interface{}, key string) map[string]interface{} {
	def, ok := value.(map[string]interface{})
	if !ok {
		def2, ok := value.(map[interface{}]interface{})
		if !ok {
			panic(fmt.Errorf("orm yaml key %s is not valid", key))
		}
		def = make(map[string]interface{})
		for k, v := range def2 {
			def[fmt.Sprintf("%v", k)] = v
		}
	}
	return def
}

func validateOrmInt(value interface{}, key string) int {
	asInt, ok := value.(int)
	if !ok {
		panic(fmt.Errorf("orm value for %s: %v is not valid", key, value))
	}
	return asInt
}

func validateOrmString(value interface{}, key string) string {
	asString, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("orm value for %s: %v is not valid", key, value))
	}
	return asString
}
