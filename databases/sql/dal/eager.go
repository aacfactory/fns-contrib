package dal

type EagerLoader[T Model] struct {
	conditions *Conditions
	orders     *Orders
	rng        *Range
}
