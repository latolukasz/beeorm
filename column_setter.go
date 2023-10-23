package beeorm

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func createNumberColumnSetter(columnName string, unsigned bool) func(v any) (string, error) {
	return func(v any) (string, error) {
		switch v.(type) {
		case string:
			_, err := strconv.ParseUint(v.(string), 10, 64)
			if err != nil {
				return "", fmt.Errorf("invalid number for column `%s`", columnName)
			}
			return v.(string), nil
		case uint8:
			return strconv.FormatUint(uint64(v.(uint8)), 10), nil
		case uint16:
			return strconv.FormatUint(uint64(v.(uint16)), 10), nil
		case uint:
			return strconv.FormatUint(uint64(v.(uint)), 10), nil
		case uint32:
			return strconv.FormatUint(uint64(v.(uint32)), 10), nil
		case uint64:
			return strconv.FormatUint(v.(uint64), 10), nil
		case int8:
			i := v.(int8)
			if i < 0 && unsigned {
				return "", fmt.Errorf("unsigned number for column `%s` not allowed", columnName)
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int16:
			i := v.(int16)
			if i < 0 && unsigned {
				return "", fmt.Errorf("unsigned number for column `%s` not allowed", columnName)
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int:
			i := v.(int)
			if i < 0 && unsigned {
				return "", fmt.Errorf("unsigned number for column `%s` not allowed", columnName)
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int32:
			i := v.(int32)
			if i < 0 && unsigned {
				return "", fmt.Errorf("unsigned number for column `%s` not allowed", columnName)
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int64:
			i := v.(int64)
			if i < 0 && unsigned {
				return "", fmt.Errorf("unsigned number for column `%s` not allowed", columnName)
			}
			return strconv.FormatInt(i, 10), nil
		}
		return "", fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
	}
}

func createStringColumnSetter(columnName string) func(v any) (string, error) {
	return func(v any) (string, error) {
		switch v.(type) {
		case string:
			return v.(string), nil
		default:
			return "", fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
		}
	}
}

func createNotSupportedColumnSetter(columnName string) func(v any) (string, error) {
	return func(v any) (string, error) {
		return "", fmt.Errorf("type %T is not supported, column `%s`", v, columnName)
	}
}

func createBoolColumnSetter(columnName string) func(v any) (string, error) {
	return func(v any) (string, error) {
		switch v.(type) {
		case bool:
			if v.(bool) {
				return "1", nil
			}
			return "0", nil
		case string:
			s := strings.ToLower(v.(string))
			if s == "1" || s == "true" {
				return "1", nil
			} else if s == "0" || s == "false" {
				return "0", nil
			}
		case int:
			asInt := v.(int)
			if asInt == 1 {
				return "1", nil
			} else if asInt == 0 {
				return "0", nil
			}
		}
		return "", fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
	}
}

func createDateTimeColumnSetter(columnName string, withTime bool) func(v any) (string, error) {
	return func(v any) (string, error) {
		asTime, isTime := v.(time.Time)
		if isTime {
			t := asTime.UTC()
			if withTime {
				return t.Format(time.DateTime), nil
			}
			return t.Format(time.DateOnly), nil
		}
		return "", fmt.Errorf("type %T not supported, column `%s`", v, columnName)
	}
}
