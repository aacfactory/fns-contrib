package oas

func emptyOpenapiSchema() *Schema {
	return &Schema{
		Key:         "github.com>aacfactory>fns>services.Empty",
		Title:       "Empty",
		Description: "Empty Object",
		Type:        "object",
	}
}
