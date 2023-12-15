package oas

func requestAuthHeadersOpenapiParams() []Parameter {
	return []Parameter{
		{
			Name:        "Authorization",
			In:          "header",
			Description: "Authorization Key",
			Required:    true,
		},
	}
}

func requestHeadersOpenapiParams() []Parameter {
	return []Parameter{
		{
			Name:        "X-Fns-Device-Id",
			In:          "header",
			Description: "Client device uuid",
			Required:    true,
		},
		{
			Name:        "X-Fns-Device-Ip",
			In:          "header",
			Description: "Client device ip",
			Required:    false,
		},
		{
			Name:        "X-Fns-Request-Id",
			In:          "header",
			Description: "request id",
			Required:    false,
		},
		{
			Name:        "X-Fns-Request-Version",
			In:          "header",
			Description: "Applicable version range, e.g.: endpointName1=v0.0.1:v1.0.0, endpointName2=v0.0.1:v1.0.0, ...",
			Required:    false,
		},
	}
}

func responseHeadersOpenapi() map[string]*Header {
	return map[string]*Header{
		"X-Fns-Endpoint-Id": {
			Description: "endpoint id",
			Schema: &Schema{
				Type: "string",
			},
		},
		"X-Fns-Endpoint-Version": {
			Description: "app version",
			Schema: &Schema{
				Type: "string",
			},
		},
		"X-Fns-Handle-Latency": {
			Description: "request latency",
			Schema: &Schema{
				Type: "string",
			},
		},
	}
}
