package postgres

func Asc(column string) *Order {
	return &Order{
		Column: column,
		Desc:   false,
	}
}

func Desc(column string) *Order {
	return &Order{
		Column: column,
		Desc:   true,
	}
}

type Order struct {
	Column string
	Desc   bool
}
