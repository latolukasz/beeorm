package modified

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/latolukasz/beeorm/v3"
)

const optionKey = "_github.com/latolukasz/beeorm/plugins/modified/AddedField"
const emptyTime = "0001-01-01 00:00:00"
const emptyDate = "0001-01-01"

type PluginOptions struct {
	AddedAtField    string
	ModifiedAtField string
}

type schemaOptions struct {
	FieldAdded       string
	FieldModified    string
	TimeAdded        bool
	TimeModified     bool
	OptionalAdded    bool
	OptionalModified bool
}

func New(addedAtField, modifiedAtField string) any {
	addedAtField = strings.TrimSpace(addedAtField)
	modifiedAtField = strings.TrimSpace(modifiedAtField)
	if addedAtField == "" && modifiedAtField == "" {
		panic(errors.New("at least one column name must be defined"))
	}
	if addedAtField != "" && strings.ToUpper(addedAtField[0:1]) != addedAtField[0:1] {
		panic(fmt.Errorf("addedAt field '%s' must be public", addedAtField))
	}
	if modifiedAtField != "" && strings.ToUpper(modifiedAtField[0:1]) != modifiedAtField[0:1] {
		panic(fmt.Errorf("modifiedAtField field '%s' must be public", modifiedAtField))
	}
	return &plugin{addedAtField: addedAtField, modifiedAtField: modifiedAtField}
}

type plugin struct {
	addedAtField    string
	modifiedAtField string
}

func (p *plugin) ValidateEntitySchema(schema beeorm.EntitySchemaSetter) error {
	fields := make([]string, 0)
	if p.addedAtField != "" {
		fields = append(fields, p.addedAtField)
	}
	if p.modifiedAtField != "" && p.modifiedAtField != p.addedAtField {
		fields = append(fields, p.modifiedAtField)
	}
	options := schemaOptions{}
	hasField := false
	for _, fieldName := range fields {
		field, has := schema.GetType().FieldByName(fieldName)
		if !has {
			continue
		}
		if schema.GetTag(fieldName, "ignore", "true", "") == "true" {
			continue
		}
		typeName := field.Type.String()
		isOptional := typeName == "*time.Time"
		if !isOptional && typeName != "time.Time" {
			continue
		}
		withTime := schema.GetTag(fieldName, "time", "true", "") == "true"
		if fieldName == p.addedAtField {
			options.FieldAdded = fieldName
			options.TimeAdded = withTime
			options.OptionalAdded = isOptional
			hasField = true
		}
		if fieldName == p.modifiedAtField {
			options.FieldModified = fieldName
			options.TimeModified = withTime
			options.OptionalModified = isOptional
			hasField = true
		}
	}
	if hasField {
		schema.SetOption(optionKey, options)
	}
	return nil
}

func (p *plugin) EntityFlush(schema beeorm.EntitySchema, entity reflect.Value, before, after beeorm.Bind, _ beeorm.Engine) (beeorm.PostFlushAction, error) {
	if after == nil && before != nil {
		return nil, nil
	}
	option := schema.Option(optionKey)
	if option == nil {
		return nil, nil
	}
	options := option.(schemaOptions)
	now := time.Now().UTC()
	if before == nil {
		if options.FieldAdded != "" {
			setDate(now, after, entity, options.FieldAdded, options.TimeAdded, options.OptionalAdded)
		}
	} else {
		if options.FieldModified != "" {
			setDate(now, after, entity, options.FieldModified, options.TimeModified, options.OptionalModified)
		}
	}
	return nil, nil
}

func setDate(now time.Time, bind beeorm.Bind, entity reflect.Value, field string, withTime, optional bool) {
	before, hasBefore := bind[field]
	if hasBefore {
		if optional {
			if before != nil {
				return
			}
		} else if withTime {
			if before != emptyTime {
				return
			}
		} else if before != emptyDate {
			return
		}
	}
	if withTime {
		now = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), 0, time.UTC)
		bind[field] = now.Format(time.DateTime)
	} else {
		now = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		bind[field] = now.Format(time.DateOnly)
	}
	if optional {
		entity.FieldByName(field).Set(reflect.ValueOf(&now))
	} else {
		entity.FieldByName(field).Set(reflect.ValueOf(now))
	}
}
