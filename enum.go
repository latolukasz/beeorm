package beeorm

import (
	"reflect"
)

type Enum string
type Set []Enum

var typeOfSet = reflect.TypeOf((Set)(nil))

type EnumValues interface {
	EnumValues() interface{}
}

type EnumDefault interface {
	Default() string
}

type enumDefinition struct {
	fields   []string
	mapping  map[string]int
	required bool
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

func initEnumDefinition(def interface{}, required bool) *enumDefinition {
	enum := &enumDefinition{required: required}
	e := reflect.ValueOf(def)
	enum.mapping = make(map[string]int)
	enum.fields = make([]string, 0)
	for i := 0; i < e.Type().NumField(); i++ {
		name := e.Field(i).String()
		enum.fields = append(enum.fields, name)
		enum.mapping[name] = i + 1
	}
	return enum
}
