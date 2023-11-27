package beeorm

import (
	"fmt"
	"math"
	"reflect"
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

func createNumberFieldBindSetter(columnName string, unsigned bool, max uint64) func(v any) (any, error) {
	return func(v any) (any, error) {
		var asUint64 uint64
		var asInt64 int64
		var err error
		switch v.(type) {
		case string:
			asUint64, err = strconv.ParseUint(v.(string), 10, 64)
			if err != nil {
				return nil, &BindError{columnName, fmt.Sprintf("invalid number for column `%s`", columnName)}
			}
		case uint8:
			asUint64 = uint64(v.(uint8))
		case uint16:
			asUint64 = uint64(v.(uint16))
		case uint:
			asUint64 = uint64(v.(uint))
		case uint32:
			asUint64 = uint64(v.(uint32))
		case uint64:
			asUint64 = v.(uint64)
		case int8:
			asInt64 = int64(v.(int8))
		case int16:
			asInt64 = int64(v.(int16))
		case int:
			asInt64 = int64(v.(int))
		case int32:
			asInt64 = int64(v.(int32))
		case int64:
			asInt64 = v.(int64)
		case float32:
			asInt64 = int64(v.(float32))
		case float64:
			asInt64 = int64(v.(float64))
		default:
			return nil, fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
		}
		if !unsigned {
			if asUint64 > 0 {
				asInt64 = int64(asUint64)
			}
			if uint64(math.Abs(float64(asInt64))) > max {
				return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded max allowed value", v)}
			}
			return asUint64, nil
		}
		if asInt64 < 0 {
			return nil, &BindError{columnName, fmt.Sprintf("negative number %d not allowed", v)}
		} else if asInt64 > 0 {
			asUint64 = uint64(asInt64)
		}
		if asUint64 > max {
			return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded max value of %d exceeded", v, max)}
		}
		return asUint64, nil
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

func createStringFieldBindSetter(columnName string, length int, required bool) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if required {
				return nil, &BindError{Field: columnName, Message: "empty string not allowed"}
			}
			return nil, nil
		}
		switch v.(type) {
		case string:
			asString := v.(string)
			if v == "" {
				if required {
					return nil, &BindError{Field: columnName, Message: "empty string not allowed"}
				}
				return nil, nil
			}
			if len(asString) > length {
				return nil, &BindError{Field: columnName,
					Message: fmt.Sprintf("text too long, max %d allowed", length)}
			}
			return asString, nil
		default:
			return nil, fmt.Errorf("invalid value `%T` for column `%s`", v, columnName)
		}
	}
}

func createStringFieldSetter(attributes schemaFieldAttributes) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		if v == nil {
			field.SetString("")
		} else {
			field.SetString(v.(string))
		}
	}
}

func createNumberFieldSetter(attributes schemaFieldAttributes, unsigned bool) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		if unsigned {
			field.SetUint(v.(uint64))
		} else {
			field.SetInt(v.(int64))
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
