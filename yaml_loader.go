package beeorm

import (
	"fmt"
	"strconv"
	"strings"
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
			case "streams":
				validateStreams(r, value, key)
			case "mysqlEncoding":
				valAsString := validateOrmString(value, key)
				r.SetDefaultEncoding(valAsString)
			case "mysqlCollate":
				valAsString := validateOrmString(value, key)
				r.SetDefaultCollate(valAsString)
			case "disableCacheHashCheck":
				if value.(bool) {
					DisableCacheHashCheck()
				}
			case "local_cache":
				number := validateOrmInt(value, key)
				r.RegisterLocalCache(number, key)
			}
		}
	}
}

func validateOrmMysqlURI(registry *Registry, value interface{}, key string) {
	asString, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("mysql uri '%v' is not valid", value))
	}
	registry.RegisterMySQLPool(asString, key)
}

func validateStreams(registry *Registry, value interface{}, key string) {
	def := fixYamlMap(value, key)
	for name, groups := range def {
		asSlice, ok := groups.([]interface{})
		if !ok {
			panic(fmt.Errorf("streams '%v' is not valid", groups))
		}
		asString := make([]string, len(asSlice))
		for i, val := range asSlice {
			asString[i] = fmt.Sprintf("%v", val)
		}
		registry.RegisterRedisStream(name, key, asString)
	}
}

func validateRedisURI(registry *Registry, value interface{}, key string) {
	asString, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	elements := strings.Split(asString, ":")
	dbNumber := ""
	uri := ""
	namespace := ""
	l := len(elements)
	switch l {
	case 2:
		dbNumber = elements[1]
		uri = elements[0]
	case 3:
		dbNumber = elements[2]
		uri = elements[0] + ":" + elements[1]
	case 4:
		dbNumber = elements[2]
		namespace = elements[3]
		uri = elements[0] + ":" + elements[1]
	default:
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	db, err := strconv.ParseUint(dbNumber, 10, 64)
	if err != nil {
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	registry.RegisterRedis(uri, namespace, int(db), key)
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
		namespace := ""
		elements := strings.Split(master, ":")
		l := len(elements)
		if l >= 2 {
			master = elements[0]
			nr, err := strconv.ParseUint(elements[1], 10, 64)
			if err != nil {
				panic(fmt.Errorf("sentinel db '%v' is not valid", value))
			}
			db = int(nr)
			if l == 3 {
				namespace = elements[2]
			}
		}
		registry.RegisterRedisSentinel(master, namespace, db, asStrings, key)
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
