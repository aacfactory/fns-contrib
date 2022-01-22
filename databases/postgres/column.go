package postgres

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql"
	"github.com/aacfactory/json"
	"reflect"
)

const (
	// kind
	pkCol            = "pk"
	incrPkCol        = "incrpk"
	normalCol        = "normal"
	conflictCol      = "conflict"
	auditCreateByCol = "acb"
	auditCreateAtCol = "act"
	auditModifyBtCol = "amb"
	auditModifyAtCol = "amt"
	auditDeleteByCol = "adb"
	auditDeleteAtCol = "adt"
	auditVersionCol  = "aol"
	virtualCol       = "vc"    // field_name,vc,"sql"
	refCol           = "ref"   // self_field_name,ref
	linkCol          = "link"  // linkName:target_field_name,link
	linksCol         = "links" // linkName:target_field_name,links,field asc,field desc,0:10
	jsonCol          = "json"  // field_name,json
)

func newColumn(host *table, kind string, conflict bool, name string, fieldName string) *column {
	return &column{
		Host:         host,
		Kind:         kind,
		Conflict:     conflict,
		Name:         name,
		FieldName:    fieldName,
		VirtualQuery: "",
	}
}

type column struct {
	Host             *table
	Kind             string
	Conflict         bool
	Name             string
	FieldName        string
	VirtualQuery     string
	RefName          string
	RefTargetColumn  *column
	Ref              *table
	LinkHostColumn   *column
	LinkTargetColumn *column
	Link             *table
	LinkOrders       []*Order
	LinkRange        *Range
}

func (c *column) queryName() string {
	return fmt.Sprintf("\"%s\"", c.Name)
}

func (c *column) generateSelect() (query string) {
	switch c.Kind {
	case virtualCol:
		query = fmt.Sprintf("(%s) AS %s", c.VirtualQuery, c.queryName())
	case refCol:
		query = fmt.Sprintf("(%s) AS %s", c.generateRefSelect(), `"`+c.RefName+`"`)
	case linkCol:
		query = fmt.Sprintf("(%s) AS %s", c.generateLinkSelect(), c.queryName())
	case linksCol:
		query = fmt.Sprintf("(%s) AS %s", c.generateLinksSelect(), c.queryName())
	default:
		query = c.Host.fullName() + "." + c.queryName()
	}
	return
}

func (c *column) generateRefSelect() (query string) {
	/*
		SELECT row_to_json("ref_table".*) FROM (
		SELECT ... FROM "schema"."ref_table" WHERE "pk" = "host_full_table_name"."ref_column" OFFSET 0 LIMIT 1
		) AS "ref_table"
	*/
	hostTableName := c.Host.fullName()
	query = `SELECT row_to_json(` + c.Ref.TableName() + `.*) FROM (`
	refQuery, _ := c.Ref.generateQuerySQL(NewConditions(Eq(c.RefTargetColumn.Name, LitValue(hostTableName+"."+c.queryName()))), NewRange(0, 1), nil)
	query = query + refQuery
	query = query + `) AS ` + c.Ref.TableName()
	return
}

func (c *column) generateLinkSelect() (query string) {
	/*
		SELECT row_to_json("ref_table".*) FROM (
		SELECT ... FROM "schema"."ref_table" WHERE "link" = "host_full_table_name"."pk" OFFSET 0 LIMIT 1
		) AS "ref_table"
	*/
	hostTableName := c.Host.fullName()
	query = `SELECT row_to_json(` + c.Link.TableName() + `.*) FROM (`
	linkQuery, _ := c.Link.generateQuerySQL(NewConditions(Eq(c.LinkTargetColumn.Name, LitValue(hostTableName+"."+c.LinkHostColumn.queryName()))), NewRange(0, 1), nil)
	query = query + linkQuery
	query = query + `) AS ` + c.Link.TableName()
	return
}

func (c *column) generateLinksSelect() (query string) {
	/*
		SELECT to_json(ARRAY(
			SELECT row_to_json("ref_table".*) FROM (
			SELECT ... FROM "schema"."ref_table" WHERE "pk" = "host_full_table_name"."ref_column" ORDER BY ... OFFSET x LIMIT y
			) AS "ref_table"
		))
	*/
	hostTableName := c.Host.fullName()
	query = `SELECT to_json(ARRAY(` + `SELECT row_to_json(` + c.Link.TableName() + `.*) FROM (`
	linksQuery, _ := c.Link.generateQuerySQL(NewConditions(Eq(c.LinkTargetColumn.Name, LitValue(hostTableName+"."+c.LinkHostColumn.queryName()))), c.LinkRange, c.LinkOrders)
	query = query + linksQuery
	query = query + `) AS ` + c.Link.TableName() + "))"
	return
}

func (c *column) isPk() (ok bool) {
	ok = c.Kind == pkCol
	return
}

func (c *column) isIncrPk() (ok bool) {
	ok = c.Kind == incrPkCol
	return
}

func (c *column) isNormal() (ok bool) {
	ok = c.Kind == normalCol
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

func (c *column) isLink() (ok bool) {
	ok = c.Kind == linkCol
	return
}

func (c *column) isLinks() (ok bool) {
	ok = c.Kind == linksCol
	return
}

func (c *column) isJson() (ok bool) {
	ok = c.Kind == jsonCol
	return
}

func mapColumnsToSqlArgs(columns []*column, rv reflect.Value, args *sql.Tuple) (err error) {
	rv = reflect.Indirect(rv)
	for _, col := range columns {
		fv := rv.FieldByName(col.FieldName)
		if col.isRef() {
			if fv.IsNil() {
				args.Append(nil)
				continue
			}
			fv = reflect.Indirect(fv)
			refValue := fv.FieldByName(col.RefTargetColumn.Name)
			args.Append(refValue.Interface())
			continue
		}
		if col.isJson() {
			if fv.IsNil() {
				args.Append(nil)
				continue
			}
			p, encodeErr := json.Marshal(fv.Interface())
			if encodeErr != nil {
				err = fmt.Errorf("encode %s column value failed, %v", col.Name, encodeErr)
				return
			}
			args.Append(p)
			continue
		}
		args.Append(fv.Interface())
	}
	return
}
