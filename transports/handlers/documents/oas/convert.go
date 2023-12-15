package oas

import (
	"fmt"
	"github.com/aacfactory/fns/services/documents"
	"sort"
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
					elementKey := ElementKey(element.Key())
					if _, hasElement := api.Components.Schemas[elementKey]; !hasElement {
						api.Components.Schemas[elementKey] = ElementSchema(element)
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
										c = ApplicationJsonContent(RefSchema("github.com>aacfactory>fns>services.Empty"))
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
										c = ApplicationJsonContent(RefSchema("github.com>aacfactory>fns>services.Empty"))
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
