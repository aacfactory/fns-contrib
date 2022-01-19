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

func NewOrders() *Orders {
	return &Orders{
		values: make([]*Order, 0, 1),
	}
}

type Orders struct {
	values []*Order
}

func (o *Orders) Asc(column string) *Orders {
	o.values = append(o.values, Asc(column))
	return o
}

func (o *Orders) Desc(column string) *Orders {
	o.values = append(o.values, Desc(column))
	return o
}

type Order struct {
	Column string
	Desc   bool
}
