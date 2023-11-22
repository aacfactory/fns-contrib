package oas

import "fmt"

func ComponentsSchemaURI(key string) (v string) {
	v = fmt.Sprintf("#/components/schemas/%s", key)
	return
}

func RefSchema(key string) (v *Schema) {
	v = &Schema{}
	v.Ref = ComponentsSchemaURI(key)
	return
}

type Schema struct {
	Key                  string             `json:"-"`
	Title                string             `json:"title,omitempty"`
	Description          string             `json:"description,omitempty"`
	Type                 string             `json:"type,omitempty"`
	Required             []string           `json:"required,omitempty"`
	Format               string             `json:"format,omitempty"`
	Enum                 []interface{}      `json:"enum,omitempty"`
	Properties           map[string]*Schema `json:"properties,omitempty"`
	Items                *Schema            `json:"items,omitempty"`
	AdditionalProperties *Schema            `json:"additionalProperties,omitempty"`
	Deprecated           bool               `json:"deprecated,omitempty"`
	Example              interface{}        `json:"example,omitempty"`
	Ref                  string             `json:"$ref,omitempty"`
}
