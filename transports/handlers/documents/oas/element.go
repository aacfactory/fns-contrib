package oas

import (
	"fmt"
	"github.com/aacfactory/fns/services/documents"
	"sort"
	"strings"
)

func ElementKey(key string) string {
	return strings.ReplaceAll(key, "/", ">")
}

func ElementSchema(element documents.Element) (v *Schema) {
	// fns
	if element.IsRef() {
		v = RefSchema(ElementKey(element.Key()))
		return
	}
	v = &Schema{
		Key:         ElementKey(element.Key()),
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
	if element.Description != "" {
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
	}

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
