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
	var parsedYaml map[string]interface{}
	err = yaml.Unmarshal(yamlFileData, &parsedYaml)
	assert.Nil(t, err)

	registry := NewRegistry()
	err = registry.InitByYaml(parsedYaml)
	assert.NoError(t, err)

	invalidYaml := make(map[string]interface{})
	invalidYaml["test"] = "invalid"
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "orm yaml key orm is not valid")

	invalidYaml = make(map[string]interface{})
	invalidYaml[DefaultPoolCode] = map[string]interface{}{"mysql": []string{}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "orm yaml key default is not valid")

	invalidYaml = make(map[string]interface{})
	invalidYaml[DefaultPoolCode] = map[string]interface{}{"redis": "invalid"}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "redis uri 'invalid' is not valid")

	invalidYaml = make(map[string]interface{})
	invalidYaml[DefaultPoolCode] = map[string]interface{}{"redis": "invalid:invalid:invalid"}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "redis uri 'invalid:invalid:invalid' is not valid")

	invalidYaml = make(map[string]interface{})
	invalidYaml[DefaultPoolCode] = map[string]interface{}{"redis": []int{1}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "redis uri '[1]' is not valid")

	invalidYaml = make(map[string]interface{})
	invalidYaml[DefaultPoolCode] = map[string]interface{}{"sentinel": map[interface{}]interface{}{"test": "wrong"}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "sentinel 'map[test:wrong]' is not valid")

	invalidYaml = make(map[string]interface{})
	invalidYaml[DefaultPoolCode] = map[string]interface{}{"sentinel": map[interface{}]interface{}{"master:wrong": []interface{}{}}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "sentinel db 'map[master:wrong:[]]' is not valid")

	invalidYaml = make(map[string]interface{})
	invalidYaml[DefaultPoolCode] = map[string]interface{}{"mysql": map[string]interface{}{"defaultEncoding": 23}}
	err = NewRegistry().InitByYaml(invalidYaml)
	assert.EqualError(t, err, "orm value for defaultEncoding: 23 is not valid")
}
