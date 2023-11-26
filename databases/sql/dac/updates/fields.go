package updates

type Field struct {
	Name  string
	Value any
}

func F(name string, value any) Fields {
	return Fields{{name, value}}
}

type Fields []Field

func (fields Fields) And(name string, value any) Fields {
	return append(fields, Field{
		name, value,
	})
}
