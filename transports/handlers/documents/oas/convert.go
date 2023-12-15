package oas

import (
	"fmt"
	"github.com/aacfactory/fns/services/documents"
	"sort"
	"strings"
)

func Openapi(title string, description string, term string, openapiVersion string, document documents.Document) (api API) {
	if openapiVersion == "" {
		openapiVersion = "3.1.0"
	}
	// oas
	api = API{
		Openapi: openapiVersion,
		Info: Info{
			Title:          title,
			Description:    description,
			TermsOfService: term,
			Contact:        nil,
			License:        nil,
			Version:        document.Version.String(),
		},
		Servers: []Server{},
		Paths:   make(map[string]Path),
		Components: Components{
			Schemas:   make(map[string]*Schema),
			Responses: make(map[string]*Response),
		},
		Tags: make([]Tag, 0, 1),
	}
	// schemas
	codeErr := codeErrOpenapiSchema()
	api.Components.Schemas[codeErr.Key] = codeErr
	jsr := jsonRawMessageOpenapiSchema()
	api.Components.Schemas[jsr.Key] = jsr
	empty := emptyOpenapiSchema()
	api.Components.Schemas[empty.Key] = empty

	for status, response := range responseStatusOpenapi() {
		api.Components.Responses[status] = response
	}
	api.Tags = append(api.Tags, Tag{
		Name:        "builtin",
		Description: "fns builtins",
	})
	healthURI, healthPathSchema := healthPath()
	api.Paths[healthURI] = healthPathSchema

	// documents
	endpoints := document.Endpoints
	if endpoints != nil || len(endpoints) > 0 {
		for _, endpoint := range endpoints {
			if !endpoint.Defined() || endpoint.Internal {
				continue
			}
			// tags
			api.Tags = append(api.Tags, Tag{
				Name:        endpoint.Name,
				Description: endpoint.Description,
			})
			// doc
			if endpoint.Elements != nil {
				for _, element := range endpoint.Elements {
					if _, hasElement := api.Components.Schemas[element.Key()]; !hasElement {
						api.Components.Schemas[element.Key()] = ElementSchema(element)
					}
				}
			}
			for _, fn := range endpoint.Functions {
				if fn.Internal {
					continue
				}
				fnDescription := fn.Description
				if fn.Errors != nil && len(fn.Errors) > 0 {
					fnDescription = fnDescription + "\n----------\n"
					fnDescription = fnDescription + "Errors:\n"
					for _, errorDocument := range fn.Errors {
						fnDescription = fnDescription + "* " + errorDocument.Name + "\n"
						i18nKeys := make([]string, 0, 1)
						for _, i18nKey := range errorDocument.Descriptions {
							i18nKeys = append(i18nKeys, i18nKey.Name)
						}
						sort.Strings(i18nKeys)
						for _, i18nKey := range i18nKeys {
							i18nVal, hasI18nValue := errorDocument.Descriptions.Get(i18nKey)
							if hasI18nValue {
								fnDescription = fnDescription + "\t* " + i18nKey + ": " + i18nVal + "\n"
							}
						}
					}
				}
				path := Path{}
				if fn.Readonly {
					path.Get = &Operation{
						OperationId: fmt.Sprintf("%s_%s", endpoint.Name, fn.Name),
						Tags:        []string{endpoint.Name},
						Summary:     fn.Title,
						Description: fnDescription,
						Deprecated:  fn.Deprecated,
						Parameters: func() []Parameter {
							params := requestHeadersOpenapiParams()
							if fn.Authorization {
								params = append(params, requestAuthHeadersOpenapiParams()...)
								return params
							}
							if fn.Param.Exist() {
								for _, property := range fn.Param.Properties {
									params = append(params, Parameter{
										Name:        property.Element.Name,
										In:          "query",
										Description: property.Element.Description,
										Required:    property.Element.Required,
									})
								}
							}
							return params
						}(),
						RequestBody: nil,
						Responses: map[string]Response{
							"200": {
								Content: func() (c map[string]*MediaType) {
									if !fn.Result.Exist() {
										c = ApplicationJsonContent(RefSchema("github.com/aacfactory/fns/service.Empty"))
										return
									}
									c = ApplicationJsonContent(ElementSchema(fn.Result))
									return
								}(),
							},
							"400": {Ref: "#/components/responses/400"},
							"401": {Ref: "#/components/responses/401"},
							"403": {Ref: "#/components/responses/403"},
							"404": {Ref: "#/components/responses/404"},
							"406": {Ref: "#/components/responses/406"},
							"408": {Ref: "#/components/responses/408"},
							"500": {Ref: "#/components/responses/500"},
							"501": {Ref: "#/components/responses/501"},
							"503": {Ref: "#/components/responses/503"},
							"555": {Ref: "#/components/responses/555"},
						},
					}
				} else {
					path.Post = &Operation{
						OperationId: fmt.Sprintf("%s_%s", endpoint.Name, fn.Name),
						Tags:        []string{endpoint.Name},
						Summary:     fn.Title,
						Description: fnDescription,
						Deprecated:  fn.Deprecated,
						Parameters: func() []Parameter {
							params := requestHeadersOpenapiParams()
							if fn.Authorization {
								params = append(params, requestAuthHeadersOpenapiParams()...)
								return params
							}
							return params
						}(),
						RequestBody: &RequestBody{
							Required:    func() bool { return fn.Param.Exist() }(),
							Description: "",
							Content: func() (c map[string]*MediaType) {
								if !fn.Param.Exist() {
									return
								}
								c = ApplicationJsonContent(ElementSchema(fn.Param))
								return
							}(),
						},
						Responses: map[string]Response{
							"200": {
								Content: func() (c map[string]*MediaType) {
									if !fn.Result.Exist() {
										c = ApplicationJsonContent(RefSchema("github.com/aacfactory/fns/service.Empty"))
										return
									}
									c = ApplicationJsonContent(ElementSchema(fn.Result))
									return
								}(),
							},
							"400": {Ref: "#/components/responses/400"},
							"401": {Ref: "#/components/responses/401"},
							"403": {Ref: "#/components/responses/403"},
							"404": {Ref: "#/components/responses/404"},
							"406": {Ref: "#/components/responses/406"},
							"408": {Ref: "#/components/responses/408"},
							"500": {Ref: "#/components/responses/500"},
							"501": {Ref: "#/components/responses/501"},
							"503": {Ref: "#/components/responses/503"},
							"555": {Ref: "#/components/responses/555"},
						},
					}
				}
				api.Paths[fmt.Sprintf("/%s/%s", endpoint.Name, fn.Name)] = path
			}
		}
	}
	return
}

func codeErrOpenapiSchema() *Schema {
	return &Schema{
		Key:         "github.com/aacfactory/errors.CodeError",
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
				AdditionalProperties: &Schema{Type: "string"},
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
			"cause": RefSchema("github.com/aacfactory/errors.CodeError"),
		},
	}
}

func jsonRawMessageOpenapiSchema() *Schema {
	return &Schema{
		Key:         "github.com/aacfactory/json.RawMessage",
		Title:       "JsonRawMessage",
		Description: "Json Raw Message",
		Type:        "object",
	}
}

func emptyOpenapiSchema() *Schema {
	return &Schema{
		Key:         "github.com/aacfactory/fns/services.Empty",
		Title:       "Empty",
		Description: "Empty Object",
		Type:        "object",
	}
}

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

func responseStatusOpenapi() map[string]*Response {
	return map[string]*Response{
		"400": {
			Description: "***BAD REQUEST***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"401": {
			Description: "***UNAUTHORIZED***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"403": {
			Description: "***FORBIDDEN***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"404": {
			Description: "***NOT FOUND***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"406": {
			Description: "***NOT ACCEPTABLE***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"408": {
			Description: "***TIMEOUT***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"500": {
			Description: "***SERVICE EXECUTE FAILED***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"501": {
			Description: "***SERVICE NOT IMPLEMENTED***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"503": {
			Description: "***SERVICE UNAVAILABLE***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
		"555": {
			Description: "***WARNING***",
			Header:      responseHeadersOpenapi(),
			Content: map[string]*MediaType{
				"Document/json": {
					Schema: RefSchema("github.com/aacfactory/errors.CodeError"),
				},
			},
		},
	}
}

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
							Key:         "github.com/aacfactory/fns/handlers.Health",
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

func ElementSchema(element documents.Element) (v *Schema) {
	// fns
	if element.IsRef() {
		v = RefSchema(element.Key())
		return
	}
	v = &Schema{
		Key:         element.Key(),
		Title:       element.Title,
		Description: "",
		Type:        element.Type,
		Required:    nil,
		Format:      element.Format,
		Enum: func(enums []string) (v []interface{}) {
			if enums == nil || len(enums) == 0 {
				return
			}
			v = make([]interface{}, 0, len(enums))
			for _, enum := range enums {
				v = append(v, enum)
			}
			return
		}(element.Enums),
		Properties:           nil,
		Items:                nil,
		AdditionalProperties: nil,
		Deprecated:           element.Deprecated,
		Ref:                  "",
	}
	// Description
	description := "### Description" + "\n"
	description = description + element.Description + " "
	if element.Validation.Enable {
		description = description + "\n\n***Validation***" + " "
		description = description + "`" + element.Validation.Name + "`" + " "
		if element.Validation.I18ns != nil && len(element.Validation.I18ns) > 0 {
			description = description + "\n"
			i18nKeys := make([]string, 0, 1)
			for _, i18n := range element.Validation.I18ns {
				i18nKeys = append(i18nKeys, i18n.Name)
			}
			sort.Strings(i18nKeys)
			for _, i18nKey := range i18nKeys {
				i18nValue, hasI18nValue := element.Validation.I18ns.Get(i18nKey)
				if hasI18nValue {
					description = description + "  " + i18nKey + ": " + i18nValue + "\n"
				}
			}
		}
	}
	if element.Enums != nil && len(element.Enums) > 0 {
		description = description + "\n\n***Enum***" + " "
		description = description + fmt.Sprintf("[%s]", strings.Join(element.Enums, ", ")) + " "
	}
	v.Description = description
	// builtin
	if element.IsBuiltin() {
		return
	}
	// object
	if element.IsObject() && !element.IsEmpty() {
		required := make([]string, 0, 1)
		v.Properties = make(map[string]*Schema)
		for _, prop := range element.Properties {
			if prop.Element.Required {
				required = append(required, prop.Name)
			}
			v.Properties[prop.Name] = ElementSchema(prop.Element)
		}
		v.Required = required
		return
	}
	// array
	if element.IsArray() {
		item, hasItem := element.GetItem()
		if hasItem {
			v.Items = ElementSchema(item)
		}
		return
	}
	// map
	if element.IsAdditional() {
		item, hasItem := element.GetItem()
		if hasItem {
			v.AdditionalProperties = ElementSchema(item)
		}
		return
	}
	return
}
