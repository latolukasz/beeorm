package beeorm

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (r *registry) InitByYaml(yaml map[string]interface{}) error {
	for key, data := range yaml {
		dataAsMap, err := fixYamlMap(data, "orm")
		if err != nil {
			return err
		}
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				err = validateOrmMysqlURI(r, value, key)
				if err != nil {
					return err
				}
			case "redis":
				err = validateRedisURI(r, value, key)
				if err != nil {
					return err
				}
			case "sentinel":
				err = validateSentinel(r, value, key)
				if err != nil {
					return err
				}
			case "local_cache":
				limit, err := validateOrmInt(value, key)
				if err != nil {
					return err
				}
				r.RegisterLocalCache(key, limit)
			}
		}
	}
	return nil
}

func validateOrmMysqlURI(registry *registry, value interface{}, key string) error {
	def, err := fixYamlMap(value, key)
	if err != nil {
		return err
	}
	uri := ""
	options := &MySQLOptions{}
	for k, v := range def {
		switch k {
		case "uri":
			uri, err = validateOrmString(v, "uri")
		case "connMaxLifetime":
			connMaxLifetime, err := validateOrmInt(v, "connMaxLifetime")
			if err != nil {
				return err
			}
			options.ConnMaxLifetime = time.Duration(connMaxLifetime) * time.Second
		case "maxOpenConnections":
			options.MaxOpenConnections, err = validateOrmInt(v, "maxOpenConnections")
			if err != nil {
				return err
			}
		case "maxIdleConnections":
			options.MaxIdleConnections, err = validateOrmInt(v, "maxIdleConnections")
			if err != nil {
				return err
			}
		case "defaultEncoding":
			options.DefaultEncoding, err = validateOrmString(v, "defaultEncoding")
			if err != nil {
				return err
			}
		case "defaultCollate":
			options.DefaultCollate, err = validateOrmString(v, "defaultCollate")
			if err != nil {
				return err
			}
		case "ignoredTables":
			options.IgnoredTables, err = validateOrmStrings(v, "ignoredTables")
			if err != nil {
				return err
			}
		}
	}
	registry.RegisterMySQL(uri, key, options)
	return nil
}

func validateRedisURI(registry *registry, value interface{}, key string) error {
	asString, ok := value.(string)
	if !ok {
		return fmt.Errorf("redis uri '%v' is not valid", value)
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
		return fmt.Errorf("redis uri '%v' is not valid", value)
	}
	db, err := strconv.ParseUint(dbNumber, 10, 64)
	if err != nil {
		return fmt.Errorf("redis uri '%v' is not valid", value)
	}
	var options *RedisOptions
	if len(parts) == 2 && parts[1] != "" {
		values, err := url.ParseQuery(parts[1])
		if err != nil {
			return fmt.Errorf("redis uri '%v' is not valid", value)
		}
		if values.Has("user") && values.Has("password") {
			options = &RedisOptions{User: values.Get("user"), Password: values.Get("password")}
		}
	}
	registry.RegisterRedis(uri, int(db), key, options)
	return nil
}

func validateSentinel(registry *registry, value interface{}, key string) error {
	def, err := fixYamlMap(value, key)
	if err != nil {
		return err
	}
	for master, values := range def {
		asSlice, ok := values.([]interface{})
		if !ok {
			return fmt.Errorf("sentinel '%v' is not valid", value)
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
				return fmt.Errorf("sentinel db '%v' is not valid", value)
			}
			db = int(nr)
		}
		options := &RedisOptions{Master: master, Sentinels: asStrings}
		if len(parts) == 2 && parts[1] != "" {
			extra, err := url.ParseQuery(parts[1])
			if err != nil {
				return fmt.Errorf("sentinel uri '%v' is not valid", master)
			}
			if extra.Has("user") && extra.Has("password") {
				options.User = extra.Get("user")
				options.Password = extra.Get("password")
			}
		}
		registry.RegisterRedis("", db, key, options)
	}
	return nil
}

func fixYamlMap(value interface{}, key string) (map[string]interface{}, error) {
	def, ok := value.(map[string]interface{})
	if !ok {
		def2, ok := value.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("orm yaml key %s is not valid", key)
		}
		def = make(map[string]interface{})
		for k, v := range def2 {
			def[fmt.Sprintf("%v", k)] = v
		}
	}
	return def, nil
}

func validateOrmInt(value interface{}, key string) (int, error) {
	asInt, ok := value.(int)
	if !ok {
		return 0, fmt.Errorf("orm value for %s: %v is not valid", key, value)
	}
	return asInt, nil
}

func validateOrmString(value interface{}, key string) (string, error) {
	asString, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("orm value for %s: %v is not valid", key, value)
	}
	return asString, nil
}

func validateOrmStrings(value interface{}, key string) ([]string, error) {
	asSlice, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("orm value for %s: %v is not valid", key, value)
	}
	asStrings := make([]string, len(asSlice))
	for i, val := range asSlice {
		asString, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("orm value for %s: %v is not valid", key, value)
		}
		asStrings[i] = asString
	}
	return asStrings, nil
}
