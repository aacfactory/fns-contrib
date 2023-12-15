package oas

func codeErrOpenapiSchema() *Schema {
	return &Schema{
		Key:         "github.com>aacfactory>errors.CodeError",
		Title:       "CodeError",
		Description: "Fns Code Error",
		Type:        "object",
		Required:    []string{"id", "code", "name", "message", "stacktrace"},
		Properties: map[string]*Schema{
			"id": {
				Title: "Id",
				Type:  "string",
			},
			"code": {
				Title: "Code",
				Type:  "string",
			},
			"name": {
				Title: "Name",
				Type:  "string",
			},
			"message": {
				Title: "Message",
				Type:  "string",
			},
			"meta": {
				Title: "Meta",
				Type:  "array",
				Items: &Schema{
					Type: "object",
					Properties: map[string]*Schema{
						"key":   {Type: "string"},
						"value": {Type: "string"},
					},
				},
			},
			"stacktrace": {
				Title: "Stacktrace",
				Type:  "object",
				Properties: map[string]*Schema{
					"fn":   {Type: "string"},
					"file": {Type: "string"},
					"line": {Type: "string"},
				},
			},
			"cause": RefSchema("github.com>aacfactory>errors.CodeError"),
		},
	}
}
