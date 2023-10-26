package beeorm

import (
	"reflect"
)

type EnumValues interface {
	EnumValues() any
}

type enumDefinition struct {
	fields       []string
	mapping      map[string]int
	required     bool
	defaultValue string
}

func (d *enumDefinition) GetFields() []string {
	return d.fields
}

func (d *enumDefinition) Has(value string) bool {
	_, has := d.mapping[value]
	return has
}

func (d *enumDefinition) Index(value string) int {
	return d.mapping[value]
}

func initEnumDefinition(def any, required bool) *enumDefinition {
	enum := &enumDefinition{required: required}
	e := reflect.ValueOf(def)
	enum.mapping = make(map[string]int)
	enum.fields = make([]string, 0)
	for i := 0; i < e.Type().NumField(); i++ {
		name := e.Field(i).String()
		enum.fields = append(enum.fields, name)
		enum.mapping[name] = i + 1
	}
	enum.defaultValue = enum.fields[0]
	return enum
}
