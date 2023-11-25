package generator

import "github.com/aacfactory/fns/cmd/generates/sources"

func BuiltinTypes() []*sources.Type {
	return []*sources.Type{
		{
			Kind: sources.StructKind,
			Path: "github.com/aacfactory/fns-contrib/databases/sql/dal",
			Name: "Pager",
			Annotations: sources.Annotations{
				sources.NewAnnotation("title", "Pager"),
				sources.NewAnnotation("description", "Paging Query Results"),
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
					Annotations: sources.Annotations{
						sources.NewAnnotation("title", "Page number"),
						sources.NewAnnotation("description", "Current page number"),
					},
					Paradigms: nil,
					Tags:      map[string]string{"json": "no"},
					Elements: []*sources.Type{{
						Kind:        sources.BasicKind,
						Path:        "",
						Name:        "int64",
						Annotations: nil,
						Paradigms:   nil,
						Tags:        nil,
						Elements:    nil,
					}},
				},
				{
					Kind: sources.StructFieldKind,
					Path: "",
					Name: "Num",
					Annotations: sources.Annotations{
						sources.NewAnnotation("title", "Page count"),
						sources.NewAnnotation("description", "Page count"),
					},
					Paradigms: nil,
					Tags:      map[string]string{"json": "num"},
					Elements: []*sources.Type{{
						Kind:        sources.BasicKind,
						Path:        "",
						Name:        "int64",
						Annotations: nil,
						Paradigms:   nil,
						Tags:        nil,
						Elements:    nil,
					}},
				},
				{
					Kind: sources.StructFieldKind,
					Path: "",
					Name: "Total",
					Annotations: sources.Annotations{
						sources.NewAnnotation("title", "Total number of entries"),
						sources.NewAnnotation("description", "Total number of entries"),
					},
					Paradigms: nil,
					Tags:      map[string]string{"json": "total"},
					Elements: []*sources.Type{{
						Kind:        sources.BasicKind,
						Path:        "",
						Name:        "int64",
						Annotations: nil,
						Paradigms:   nil,
						Tags:        nil,
						Elements:    nil,
					}},
				},
				{
					Kind: sources.StructFieldKind,
					Path: "",
					Name: "Items",
					Annotations: sources.Annotations{
						sources.NewAnnotation("title", "Page entry"),
						sources.NewAnnotation("description", "Page entry"),
					},
					Paradigms: nil,
					Tags:      map[string]string{"json": "items"},
					Elements: []*sources.Type{{
						Kind:        sources.ArrayKind,
						Path:        "",
						Name:        "",
						Annotations: nil,
						Paradigms:   nil,
						Tags:        nil,
						Elements: []*sources.Type{{
							Kind:        sources.ParadigmElementKind,
							Path:        "",
							Name:        "E",
							Annotations: nil,
							Paradigms:   nil,
							Tags:        nil,
							Elements:    []*sources.Type{sources.AnyType},
						}},
					}},
				},
			},
		},
	}
}
