package postgres

import (
	"container/list"
	"context"
)

type edgeElement struct {
	node *table
	from *column
	to   *column
}

func setEdgeChain(ctx context.Context, edge *edgeChain) context.Context {
	return context.WithValue(ctx, "edge", edge)
}

func getEdgeChain(ctx context.Context) (edge *edgeChain, has bool) {
	v := ctx.Value("edge")
	if v == nil {
		return
	}
	edge, has = v.(*edgeChain)
	return
}

func newEdgeChain(t *table) *edgeChain {
	l := list.New()
	l.PushBack(&edgeElement{
		node: t,
	})
	return &edgeChain{
		value: l,
	}
}

type edgeChain struct {
	value *list.List
}

func (edge *edgeChain) Push(n *table, from *column, to *column) {
	edge.value.PushBack(&edgeElement{
		node: n,
		from: from,
		to:   to,
	})
}

func (edge *edgeChain) Fork() (fork *edgeChain) {
	fork = &edgeChain{
		value: list.New(),
	}
	fork.value.PushBackList(edge.value)
	return
}

func (edge *edgeChain) Head() (n *table) {
	n = edge.value.Front().Value.(*edgeElement).node
	return
}

func (edge *edgeChain) Tail() (n *table, from *column, to *column) {
	e := edge.value.Back().Value.(*edgeElement)
	n = e.node
	from = e.from
	to = e.to
	return
}

func (edge *edgeChain) Contains(n *table) (has bool) {
	size := edge.value.Len()
	if size <= 1 {
		return
	}
	front := edge.value.Front()
	for i := 1; i < size; i++ {
		if front == nil {
			continue
		}
		has = n.fullName() == front.Value.(*edgeElement).node.fullName()
		if has {
			return
		}
		front = front.Next()
	}
	return
}
