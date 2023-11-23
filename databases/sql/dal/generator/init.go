package generator

import "github.com/aacfactory/fns/cmd/generates/sources"

func init() {

}

func registerTypes() {
	sources.RegisterBuiltinType(&sources.Type{
		Kind: sources.StructKind,
		Path: "github.com/aacfactory/fns-contrib/databases/sql/dal",
		Name: "Pager",
		Annotations: Annotations{
			NewAnnotation("title", "Pager"),
			NewAnnotation("description", "Paging Query Results"),
		},
		Paradigms: []*sources.TypeParadigm{{
			Name:  "E",
			Types: []*sources.Type{sources.AnyType},
		}},
		Tags: nil,
		Elements: []*sources.Type{
			{
				Kind: sources.StructFieldKind,
				Path: "",
				Name: "No",
				Annotations: Annotations{
					NewAnnotation("title", "Page number"),
					NewAnnotation("description", "Current page number"),
				},
				Paradigms: nil,
				Tags:      map[string]string{"json": "no"},
				Elements: []*Type{{
					Kind:        BasicKind,
					Path:        "",
					Name:        "int64",
					Annotations: nil,
					Paradigms:   nil,
					Tags:        nil,
					Elements:    nil,
				}},
			},
			{
				Kind: StructFieldKind,
				Path: "",
				Name: "Num",
				Annotations: Annotations{
					NewAnnotation("title", "Page count"),
					NewAnnotation("description", "Page count"),
				},
				Paradigms: nil,
				Tags:      map[string]string{"json": "num"},
				Elements: []*Type{{
					Kind:        BasicKind,
					Path:        "",
					Name:        "int64",
					Annotations: nil,
					Paradigms:   nil,
					Tags:        nil,
					Elements:    nil,
				}},
			},
			{
				Kind: StructFieldKind,
				Path: "",
				Name: "Total",
				Annotations: Annotations{
					NewAnnotation("title", "Total number of entries"),
					NewAnnotation("description", "Total number of entries"),
				},
				Paradigms: nil,
				Tags:      map[string]string{"json": "total"},
				Elements: []*Type{{
					Kind:        BasicKind,
					Path:        "",
					Name:        "int64",
					Annotations: nil,
					Paradigms:   nil,
					Tags:        nil,
					Elements:    nil,
				}},
			},
			{
				Kind: StructFieldKind,
				Path: "",
				Name: "Items",
				Annotations: Annotations{
					NewAnnotation("title", "Page entry"),
					NewAnnotation("description", "Page entry"),
				},
				Paradigms: nil,
				Tags:      map[string]string{"json": "items"},
				Elements: []*Type{{
					Kind:        ArrayKind,
					Path:        "",
					Name:        "",
					Annotations: nil,
					Paradigms:   nil,
					Tags:        nil,
					Elements: []*Type{{
						Kind:        ParadigmElementKind,
						Path:        "",
						Name:        "E",
						Annotations: nil,
						Paradigms:   nil,
						Tags:        nil,
						Elements:    []*Type{AnyType},
					}},
				}},
			},
		},
	})
}
