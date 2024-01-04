package beeorm

import (
	"reflect"
	"strings"
)

type Where interface {
	String() string
	GetParameters() []any
}

type BaseWhere struct {
	query      string
	parameters []any
}

func (w *BaseWhere) String() string {
	return w.query
}

func (w *BaseWhere) SetParameter(index int, param any) *BaseWhere {
	w.parameters[index-1] = param
	return w
}

func (w *BaseWhere) SetParameters(params ...any) *BaseWhere {
	w.parameters = params
	return w
}

func (w *BaseWhere) GetParameters() []any {
	return w.parameters
}

func (w *BaseWhere) Append(query string, parameters ...any) {
	newWhere := NewWhere(query, parameters...)
	if !strings.HasPrefix(query, " ") {
		w.query += " "
	}
	w.query += newWhere.query
	w.parameters = append(w.parameters, newWhere.parameters...)
}

func NewWhere(query string, parameters ...any) *BaseWhere {
	finalParameters := make([]any, 0, len(parameters))
	for _, value := range parameters {
		switch reflect.TypeOf(value).Kind().String() {
		case "slice", "array":
			val := reflect.ValueOf(value)
			length := val.Len()
			in := strings.Repeat(",?", length)
			in = strings.TrimLeft(in, ",")
			query = strings.Replace(query, "IN ?", "IN ("+in+")", 1)
			for i := 0; i < length; i++ {
				finalParameters = append(finalParameters, val.Index(i).Interface())
			}
			continue
		}
		finalParameters = append(finalParameters, value)
	}
	return &BaseWhere{query, finalParameters}
}
