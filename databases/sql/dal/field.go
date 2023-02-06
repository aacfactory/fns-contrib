package dal

import (
	"strings"
)

const (
	tag = "col"
)

const (
	pkKindField         = "PK"
	incrKindPkField     = "INCRPK"
	normalKindField     = "NORMAL"
	conflictKindField   = "CONFLICT"
	jsonObjectKindField = "JSON"
	acbKindField        = "ACB"
	actKindField        = "ACT"
	ambKindField        = "AMB"
	amtKindField        = "AMT"
	adbKindField        = "ADB"
	adtKindField        = "ADT"
	aolKindField        = "AOL"
	virtualKindField    = "VC"
	referenceKindField  = "REF"
	linkKindField       = "LINK"
	linksKindField      = "LINKS"
	treeField           = "TREE"
)

type Field struct {
	kind      string
	conflict  bool
	name      string
	model     *ModelStructure
	columns   []string
	reference *ReferenceField
	link      *LinkField
	virtual   *VirtualField
	tree      *TreeNodeField
}

func (field *Field) Model() (model *ModelStructure) {
	model = field.model
	return
}

func (field *Field) Name() (name string) {
	name = field.name
	return
}

func (field *Field) Column() (column string) {
	if field.columns == nil || len(field.columns) == 0 {
		return
	}
	column = field.columns[0]
	return
}

func (field *Field) Columns() (columns []string) {
	columns = field.columns
	return
}

func (field *Field) HasColumns() (has bool) {
	has = field.columns != nil && len(field.columns) > 0
	return
}

func (field *Field) Conflict() (ok bool) {
	ok = field.conflict
	return
}

func (field *Field) Reference() (reference *ReferenceField) {
	reference = field.reference
	return
}

func (field *Field) Link() (link *LinkField) {
	link = field.link
	return
}

func (field *Field) Virtual() (virtual *VirtualField) {
	virtual = field.virtual
	return
}

func (field *Field) Tree() (tree *TreeNodeField) {
	tree = field.tree
	return
}

func (field *Field) IsPk() (ok bool) {
	ok = field.kind == pkKindField
	return
}

func (field *Field) IsIncrPk() (ok bool) {
	ok = field.kind == incrKindPkField
	return
}

func (field *Field) IsNormal() (ok bool) {
	ok = field.kind == normalKindField
	return
}

func (field *Field) IsJson() (ok bool) {
	ok = field.kind == jsonObjectKindField
	return
}

func (field *Field) IsACB() (ok bool) {
	ok = field.kind == acbKindField
	return
}

func (field *Field) IsACT() (ok bool) {
	ok = field.kind == actKindField
	return
}

func (field *Field) IsAMB() (ok bool) {
	ok = field.kind == ambKindField
	return
}

func (field *Field) IsAMT() (ok bool) {
	ok = field.kind == amtKindField
	return
}

func (field *Field) IsADB() (ok bool) {
	ok = field.kind == adbKindField
	return
}

func (field *Field) IsADT() (ok bool) {
	ok = field.kind == adtKindField
	return
}

func (field *Field) IsAOL() (ok bool) {
	ok = field.kind == aolKindField
	return
}

func (field *Field) IsVirtual() (ok bool) {
	ok = field.kind == virtualKindField
	return
}

func (field *Field) IsReference() (ok bool) {
	ok = field.kind == referenceKindField
	return
}

func (field *Field) IsLink() (ok bool) {
	ok = field.kind == linkKindField || field.kind == linksKindField
	return
}

func (field *Field) IsTreeType() (ok bool) {
	ok = field.kind == treeField
	return
}

type TreeNodeField struct {
	nodeColumnName   string
	parentColumnName string
}

func (t *TreeNodeField) RefColumnName() (node string, parent string) {
	node = t.nodeColumnName
	parent = t.parentColumnName
	return
}

type ReferenceField struct {
	name          string
	targetModel   *ModelStructure
	targetFields  []*Field
	targetColumns []string
	abstracted    bool
}

func (r *ReferenceField) Name() (name string) {
	name = r.name
	return
}

func (r *ReferenceField) Target() (targetModel *ModelStructure, targetFields []*Field, columns []string) {
	targetModel, targetFields, columns = r.targetModel, r.targetFields, r.targetColumns
	return
}

func (r *ReferenceField) Abstracted() (ok bool) {
	ok = r.abstracted
	return
}

func (r *ReferenceField) scanAbstracted(rp *ModelStructureReferencePath) {
	if r.abstracted {
		return
	}
	nrp := rp.mount(r.targetModel)
	r.abstracted = nrp.hasParent(r.targetModel)
	if r.abstracted {
		return
	}
	r.targetModel.scanAbstractedFields(nrp)
	return
}

type LinkField struct {
	name          string
	arrayed       bool
	targetModel   *ModelStructure
	targetFields  []*Field
	targetColumns []string
	abstracted    bool
	orders        *Orders
	rng           *Range
}

func (l *LinkField) Name() (name string) {
	name = l.name
	return
}

func (l *LinkField) Target() (targetModel *ModelStructure, columns []string, orders *Orders, rng *Range) {
	targetModel, columns, orders, rng = l.targetModel, l.targetColumns, l.orders, l.rng
	return
}

func (l *LinkField) Abstracted() (ok bool) {
	ok = l.abstracted
	return
}

func (l *LinkField) Arrayed() (ok bool) {
	ok = l.arrayed
	return
}

func (l *LinkField) scanAbstracted(rp *ModelStructureReferencePath) {
	if l.abstracted {
		return
	}
	nrp := rp.mount(l.targetModel)
	l.abstracted = nrp.hasParent(l.targetModel)
	if l.abstracted {
		return
	}
	l.targetModel.scanAbstractedFields(nrp)
	return
}

type VirtualField struct {
	name  string
	query string
}

func (v *VirtualField) Name() (name string) {
	name = v.name
	return
}

func (v *VirtualField) Query() (query string) {
	query = v.query
	return
}

func scanReferenceOrLinkColumns(columns string) (v []string) {
	v = make([]string, 0, 1)
	columns = strings.TrimSpace(columns)
	if columns[0] == '[' {
		columns = columns[0 : len(columns)-1]
		items := strings.Split(columns, ",")
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			v = append(v, item)
		}
	} else {
		v = append(v, columns)
	}
	return
}
