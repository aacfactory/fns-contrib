package orders

type Order struct {
	Name string
	Desc bool
}

type Orders []Order

func (o Orders) Asc(name string) Orders {
	return append(o, Order{Name: name, Desc: false})
}

func (o Orders) Desc(name string) Orders {
	return append(o, Order{Name: name, Desc: true})
}

func Asc(name string) Orders {
	return Orders{{
		Name: name,
		Desc: false,
	}}
}

func Desc(name string) Orders {
	return Orders{{
		Name: name,
		Desc: true,
	}}
}
