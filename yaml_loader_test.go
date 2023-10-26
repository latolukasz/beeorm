package beeorm

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestYamlLoader(t *testing.T) {
	yamlFileData, err := os.ReadFile("./config.yaml")
	assert.Nil(t, err)
	var parsedYaml map[string]any
	err = yaml.Unmarshal(yamlFileData, &parsedYaml)
	assert.Nil(t, err)

	registry := NewRegistry()
	err = registry.InitByYaml(parsedYaml)
	assert.NoError(t, err)

	invalidYaml := make(map[string]any)
	invalidYaml["test"] = "invalid"
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "orm yaml key orm is not valid")

	invalidYaml = make(map[string]any)
	invalidYaml[DefaultPoolCode] = map[string]any{"mysql": []string{}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "orm yaml key default is not valid")

	invalidYaml = make(map[string]any)
	invalidYaml[DefaultPoolCode] = map[string]any{"redis": "invalid"}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "redis uri 'invalid' is not valid")

	invalidYaml = make(map[string]any)
	invalidYaml[DefaultPoolCode] = map[string]any{"redis": "invalid:invalid:invalid"}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "redis uri 'invalid:invalid:invalid' is not valid")

	invalidYaml = make(map[string]any)
	invalidYaml[DefaultPoolCode] = map[string]any{"redis": []int{1}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "redis uri '[1]' is not valid")

	invalidYaml = make(map[string]any)
	invalidYaml[DefaultPoolCode] = map[string]any{"sentinel": map[any]any{"test": "wrong"}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "sentinel 'map[test:wrong]' is not valid")

	invalidYaml = make(map[string]any)
	invalidYaml[DefaultPoolCode] = map[string]any{"sentinel": map[any]any{"master:wrong": []any{}}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "sentinel db 'map[master:wrong:[]]' is not valid")

	invalidYaml = make(map[string]any)
	invalidYaml[DefaultPoolCode] = map[string]any{"mysql": map[string]any{"defaultEncoding": 23}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "orm value for defaultEncoding: 23 is not valid")
}
