package dal

import "fmt"

func newModelStructureReferencePath(structure *ModelStructure) (rp *ModelStructureReferencePath) {
	rp = &ModelStructureReferencePath{
		current: fmt.Sprintf("%s.%s", structure.schema, structure.name),
		parent:  nil,
	}
	return
}

type ModelStructureReferencePath struct {
	current string
	parent  *ModelStructureReferencePath
}

func (rp *ModelStructureReferencePath) mount(structure *ModelStructure) (n *ModelStructureReferencePath) {
	n = newModelStructureReferencePath(structure)
	n.parent = rp
	return
}

func (rp *ModelStructureReferencePath) hasParent(structure *ModelStructure) (has bool) {
	if rp.parent == nil {
		return
	}
	if rp.parent.current == fmt.Sprintf("%s.%s", structure.schema, structure.name) {
		has = true
		return
	}
	has = rp.parent.hasParent(structure)
	return
}
