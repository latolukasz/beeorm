package beeorm

import (
	"fmt"
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
				return "", &BindError{columnName, "invalid number"}
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
				return "", &BindError{columnName, "unsigned number not allowed"}
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int16:
			i := v.(int16)
			if i < 0 && unsigned {
				return "", &BindError{columnName, "unsigned number not allowed"}
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int:
			i := v.(int)
			if i < 0 && unsigned {
				return "", &BindError{columnName, "unsigned number not allowed"}
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int32:
			i := v.(int32)
			if i < 0 && unsigned {
				return "", &BindError{columnName, "unsigned number not allowed"}
			}
			return strconv.FormatInt(int64(i), 10), nil
		case int64:
			i := v.(int64)
			if i < 0 && unsigned {
				return "", &BindError{columnName, "unsigned number not allowed"}
			}
			return strconv.FormatInt(i, 10), nil
		}
		return "", &BindError{columnName, "invalid value"}
	}
}

func createNumberFieldBindSetter(columnName string, unsigned, nullable bool, min int64, max uint64) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		var asUint64 uint64
		var asInt64 int64
		var err error
		switch v.(type) {
		case string:
			if unsigned {
				asUint64, err = strconv.ParseUint(v.(string), 10, 64)
				if err != nil {
					return nil, &BindError{columnName, fmt.Sprintf("invalid number %s", v.(string))}
				}
			} else {
				asInt64, err = strconv.ParseInt(v.(string), 10, 64)
				if err != nil {
					return nil, &BindError{columnName, fmt.Sprintf("invalid number %s", v.(string))}
				}
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
			return nil, &BindError{columnName, "invalid value"}
		}
		if !unsigned {
			if asUint64 > 0 {
				asInt64 = int64(asUint64)
			}
			if asInt64 > 0 && uint64(asInt64) > max {
				return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded max allowed value", v)}
			}
			if asInt64 < 0 && asInt64 < min {
				return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded min allowed value", v)}
			}
			return asInt64, nil
		}
		if asInt64 < 0 {
			return nil, &BindError{columnName, fmt.Sprintf("negative number %d not allowed", v)}
		} else if asInt64 > 0 {
			asUint64 = uint64(asInt64)
		}
		if asUint64 > max {
			return nil, &BindError{columnName, fmt.Sprintf("value %d exceeded max allowed value", v)}
		}
		return asUint64, nil
	}
}

func createReferenceFieldBindSetter(columnName string, idSetter fieldBindSetter, nullable bool) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		reference, is := v.(referenceInterface)
		if is {
			id := reference.getID()
			if id == 0 {
				if !nullable {
					return nil, &BindError{columnName, "nil is not allowed"}
				}
				return nil, nil
			}
			return id, nil
		}
		id, err := idSetter(v)
		if err != nil {
			return nil, err
		}
		if id == uint64(0) {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		return id, nil
	}
}

func createEnumFieldBindSetter(columnName string, stringSetter fieldBindSetter, def *enumDefinition) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil || v == "" {
			if def.required {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		v = fmt.Sprintf("%s", v)
		val, err := stringSetter(v)
		if err != nil {
			return nil, err
		}
		if !def.Has(val.(string)) {
			return nil, &BindError{columnName, fmt.Sprintf("invalid value: %s", v)}
		}
		return val, nil
	}
}

func createSetFieldBindSetter(columnName string, enumSetter fieldBindSetter, def *enumDefinition) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil || v == "" {
			if def.required {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		toReturn := strings.Trim(strings.ReplaceAll(fmt.Sprintf("%v", v), " ", ","), "[]")
		if toReturn == "" {
			if def.required {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		values := strings.Split(toReturn, ",")
		for _, option := range values {
			_, err := enumSetter(option)
			if err != nil {
				return nil, err
			}
		}
		return toReturn, nil
	}
}

func createStringColumnSetter(columnName string) func(v any) (string, error) {
	return func(v any) (string, error) {
		switch v.(type) {
		case string:
			return v.(string), nil
		default:
			return "", &BindError{columnName, "invalid value"}
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
			if length > 0 && len(asString) > length {
				return nil, &BindError{Field: columnName,
					Message: fmt.Sprintf("text too long, max %d allowed", length)}
			}
			return asString, nil
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
	}
}

func createBytesFieldBindSetter(columnName string) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			return nil, nil
		}
		switch v.(type) {
		case string:
			asString := v.(string)
			if v == "" {
				return nil, nil
			}
			return asString, nil
		case []byte:
			asString := string(v.([]byte))
			if v == "" {
				return nil, nil
			}
			return asString, nil
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
	}
}

func createBoolFieldBindSetter(columnName string) func(v any) (any, error) {
	return func(v any) (any, error) {
		switch v.(type) {
		case bool:
			return v, nil
		case int:
			return v.(int) == 1, nil
		case string:
			s := strings.ToLower(v.(string))
			return s == "true" || s == "1" || s == "yes", nil
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
	}
}

func createFloatFieldBindSetter(columnName string, unsigned, nullable bool, floatsPrecision, floatsSize, floatsDecimalSize int) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			if !nullable {
				return nil, &BindError{columnName, "nil is not allowed"}
			}
			return nil, nil
		}
		var asFloat64 float64
		var err error
		switch v.(type) {
		case string:
			asFloat64, err = strconv.ParseFloat(v.(string), 10)
			if err != nil {
				return nil, &BindError{columnName, fmt.Sprintf("invalid number %s", v.(string))}
			}
		case uint8:
			asFloat64 = float64(v.(uint8))
		case uint16:
			asFloat64 = float64(v.(uint16))
		case uint:
			asFloat64 = float64(v.(uint))
		case uint32:
			asFloat64 = float64(v.(uint32))
		case uint64:
			asFloat64 = float64(v.(uint64))
		case int8:
			asFloat64 = float64(v.(int8))
		case int16:
			asFloat64 = float64(v.(int16))
		case int:
			asFloat64 = float64(v.(int))
		case int32:
			asFloat64 = float64(v.(int32))
		case int64:
			asFloat64 = float64(v.(int64))
		case float32:
			asFloat64 = float64(v.(float32))
		case float64:
			asFloat64 = v.(float64)
		default:
			return nil, &BindError{columnName, "invalid value"}
		}
		if unsigned && asFloat64 < 0 {
			return nil, &BindError{columnName, fmt.Sprintf("negative number %d not allowed", v)}
		}
		if asFloat64 == 0 {
			return "0", nil
		}
		roundV := roundFloat(asFloat64, floatsPrecision)
		val := strconv.FormatFloat(roundV, 'f', floatsPrecision, floatsSize)
		decimalSize := floatsDecimalSize
		if decimalSize != -1 && strings.Index(val, ".") > decimalSize {
			return nil, &BindError{Field: columnName, Message: fmt.Sprintf("decimal size too big, max %d allowed", decimalSize)}
		}
		return val, nil
	}
}

func createBoolNullableFieldBindSetter(boolSetter fieldBindSetter) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			return nil, nil
		}
		return boolSetter(v)
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

func createBytesFieldSetter(attributes schemaFieldAttributes) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		if v == nil {
			field.SetZero()
		} else {
			field.SetBytes([]byte(v.(string)))
		}
	}
}

func createBoolFieldSetter(attributes schemaFieldAttributes) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		field.SetBool(v.(bool))
	}
}

func createFloatFieldSetter(attributes schemaFieldAttributes) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		f, _ := strconv.ParseFloat(v.(string), 64)
		field.SetFloat(f)
	}
}

func createBoolNullableFieldSetter(attributes schemaFieldAttributes) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		if v == nil {
			field.SetZero()
			return
		}
		val := reflect.New(field.Type().Elem())
		val.Elem().SetBool(v.(bool))
		field.Set(val)
	}
}

func createSetFieldSetter(attributes schemaFieldAttributes) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		if v == nil {
			field.SetZero()
			return
		}
		values := strings.Split(v.(string), ",")
		val := reflect.MakeSlice(field.Type(), len(values), len(values))
		for i, value := range values {
			val.Index(i).SetString(value)
		}
		field.Set(val)
	}
}

func createNumberFieldSetter(attributes schemaFieldAttributes, unsigned, nullable bool) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		if v == nil {
			field.SetZero()
			return
		}
		if nullable {
			val := reflect.New(field.Type().Elem())
			if unsigned {
				val.Elem().SetUint(v.(uint64))
			} else {
				val.Elem().SetInt(v.(int64))
			}
			field.Set(val)
			return
		}
		if unsigned {
			field.SetUint(v.(uint64))
			return
		}
		field.SetInt(v.(int64))
	}
}

func createReferenceFieldSetter(attributes schemaFieldAttributes) func(v any, elem reflect.Value) {
	return func(v any, elem reflect.Value) {
		field := elem
		for _, i := range attributes.Parents {
			field = field.Field(i)
		}
		field = field.Field(attributes.Index)
		if v == nil {
			field.SetZero()
			return
		}
		val := reflect.New(field.Type().Elem())
		reference := val.Interface().(referenceInterface)
		reference.setID(v.(uint64))
		field.Set(val)
	}
}

func createNotSupportedColumnSetter(columnName string) func(v any) (string, error) {
	return func(v any) (string, error) {
		return "", &BindError{columnName, fmt.Sprintf("type %T is not supported", v)}
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
		return "", &BindError{columnName, "invalid value"}
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
		return "", &BindError{columnName, "invalid value"}
	}
}
