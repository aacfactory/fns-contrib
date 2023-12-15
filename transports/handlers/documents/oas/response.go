package oas

func responseStatusOpenapi() map[string]*Response {
	return map[string]*Response{
		"400": {
			Description: "***BAD REQUEST***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"401": {
			Description: "***UNAUTHORIZED***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"403": {
			Description: "***FORBIDDEN***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"404": {
			Description: "***NOT FOUND***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"406": {
			Description: "***NOT ACCEPTABLE***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"408": {
			Description: "***TIMEOUT***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"500": {
			Description: "***SERVICE EXECUTE FAILED***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"501": {
			Description: "***SERVICE NOT IMPLEMENTED***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"503": {
			Description: "***SERVICE UNAVAILABLE***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
		"555": {
			Description: "***WARNING***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"application/json": {
					Schema: RefSchema("github.com>aacfactory>errors.CodeError"),
				},
			},
		},
	}
}
