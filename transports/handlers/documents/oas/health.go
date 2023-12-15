package oas

func healthPath() (uri string, path Path) {
	uri = "/health"
	path = Path{
		Get: &Operation{
			OperationId: "application_health",
			Tags:        []string{"builtin"},
			Summary:     "Health Check",
			Description: "Check fns system health status",
			Deprecated:  false,
			Parameters:  nil,
			RequestBody: nil,
			Responses: map[string]Response{
				"200": {
					Content: func() (c map[string]*MediaType) {
						schema := &Schema{
							Key:         "github.com>aacfactory>fns>runtime.Health",
							Title:       "Health Check Result",
							Description: "",
							Type:        "object",
							Required:    []string{"name", "id", "version", "running", "now"},
							Properties: map[string]*Schema{
								"name": {
									Title: "Application name",
									Type:  "string",
								},
								"id": {
									Title: "Application id",
									Type:  "string",
								},
								"version": {
									Title: "Application version",
									Type:  "string",
								},
								"running": {
									Title: "Application running status",
									Type:  "boolean",
								},
								"launch": {
									Title:                "Application launch times",
									Type:                 "string",
									Format:               "2006-01-02T15:04:05Z07:00",
									AdditionalProperties: &Schema{Type: "string"},
								},
								"now": {
									Title:                "Now",
									Type:                 "string",
									Format:               "2006-01-02T15:04:05Z07:00",
									AdditionalProperties: &Schema{Type: "string"},
								},
							},
						}
						c = ApplicationJsonContent(schema)
						return
					}(),
				},
				"503": {Ref: "#/components/responses/503"},
			},
		},
	}
	return
}
