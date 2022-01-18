package postgres

import "fmt"

const (
	// kind
	pkCol            = "pk"
	incrPkCol        = "incrPk"
	auditCreateByCol = "acb"
	auditCreateAtCol = "act"
	auditModifyBtCol = "amb"
	auditModifyAtCol = "amt"
	auditDeleteByCol = "adb"
	auditDeleteAtCol = "adt"
	auditVersionCol  = "aol"
	virtualCol       = "vc"   // field_name,vc,"sql"
	refCol           = "ref"  // field_name,ref,field asc,field desc,0:10
	refsCol          = "refs" // field_name,refs,field asc,field desc,0:10
	jsonCol          = "json" // field_name,json
)

type column struct {
	Kind      string
	Name      string
	FieldName string
	SourceSQL string
}

func (c *column) sqlName() string {
	return fmt.Sprintf("\"%s\"", c.Name)
}

func (c *column) isPk() (ok bool) {
	ok = c.Kind == pkCol
	return
}

func (c *column) isIncrPk() (ok bool) {
	ok = c.Kind == incrPkCol
	return
}

func (c *column) isAcb() (ok bool) {
	ok = c.Kind == auditCreateByCol
	return
}

func (c *column) isAct() (ok bool) {
	ok = c.Kind == auditCreateAtCol
	return
}

func (c *column) isAmb() (ok bool) {
	ok = c.Kind == auditModifyBtCol
	return
}

func (c *column) isAmt() (ok bool) {
	ok = c.Kind == auditModifyAtCol
	return
}

func (c *column) isAdb() (ok bool) {
	ok = c.Kind == auditDeleteByCol
	return
}

func (c *column) isAdt() (ok bool) {
	ok = c.Kind == auditDeleteAtCol
	return
}

func (c *column) isAol() (ok bool) {
	ok = c.Kind == auditVersionCol
	return
}

func (c *column) isVc() (ok bool) {
	ok = c.Kind == virtualCol
	return
}

func (c *column) isRef() (ok bool) {
	ok = c.Kind == refCol
	return
}

func (c *column) isRefs() (ok bool) {
	ok = c.Kind == refsCol
	return
}

func (c *column) isJson() (ok bool) {
	ok = c.Kind == jsonCol
	return
}
