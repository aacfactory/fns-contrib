package generators

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/gcg"
	"strings"
)

// TransactionWriter
// @mysql:transaction {readonly} {isolation}
// isolation:
// - ReadCommitted
// - ReadUncommitted
// - WriteCommitted
// - RepeatableRead
// - Snapshot
// - Serializable
// - Linearizable
type TransactionWriter struct {
}

func (writer *TransactionWriter) Annotation() (annotation string) {
	return "mysql:transaction"
}

func (writer *TransactionWriter) HandleBefore(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	paramsLen := len(params)
	if paramsLen > 2 {
		err = errors.Warning("sql: generate transaction code failed").WithCause(fmt.Errorf("invalid annotation params"))
		return
	}
	readonly := false
	isolationParam := ""
	isolation := sql.LevelDefault
	if paramsLen == 2 {
		if strings.ToLower(params[0]) != "readonly" {
			err = errors.Warning("sql: generate transaction code failed").WithCause(fmt.Errorf("invalid annotation params"))
			return
		}
		readonly = true
		isolationParam = strings.ToLower(params[1])
	} else if paramsLen == 1 {
		param := strings.ToLower(params[0])
		if param == "readonly" {
			readonly = true
		} else {
			isolationParam = strings.ToLower(params[0])
		}
	}
	if isolationParam != "" {
		switch isolationParam {
		case "readcommitted":
			isolation = sql.LevelReadCommitted
			break
		case "readuncommitted":
			isolation = sql.LevelReadUncommitted
			break
		case "writecommitted":
			isolation = sql.LevelWriteCommitted
			break
		case "repeatableread":
			isolation = sql.LevelRepeatableRead
			break
		case "snapshot":
			isolation = sql.LevelSnapshot
			break
		case "serializable":
			isolation = sql.LevelSerializable
			break
		case "Linearizable":
			isolation = sql.LevelLinearizable
			break
		default:
			err = errors.Warning("sql: generate transaction code failed").WithCause(fmt.Errorf("invalid isolation params"))
			return
		}
	}
	stmt := gcg.Statements()

	stmt.Tab().Token("if err = mysql.Begin(ctx")
	if readonly {
		stmt.Token(", mysql.Readonly()")
	}
	if isolation != sql.LevelDefault {
		stmt.Token(", mysql.WithIsolation(")
		switch isolation {
		case sql.LevelReadCommitted:
			stmt.Token("mysql.LevelReadCommitted")
			break
		case sql.LevelReadUncommitted:
			stmt.Token("mysql.LevelReadUncommitted")
			break
		case sql.LevelWriteCommitted:
			stmt.Token("mysql.LevelWriteCommitted")
			break
		case sql.LevelRepeatableRead:
			stmt.Token("mysql.LevelRepeatableRead")
			break
		case sql.LevelSnapshot:
			stmt.Token("mysql.LevelSnapshot")
			break
		case sql.LevelSerializable:
			stmt.Token("mysql.LevelSerializable")
			break
		case sql.LevelLinearizable:
			stmt.Token("mysql.LevelLinearizable")
			break
		default:
			stmt.Token("mysql.LevelDefault")
			break
		}
		stmt.Token(")")
	}
	stmt.Token("); err != nil {",
		gcg.NewPackage("github.com/aacfactory/fns-contrib/databases/mysql"),
	).Line()
	stmt.Tab().Tab().Token("return")
	stmt.Tab().Token("}")
	code = stmt
	return
}

func (writer *TransactionWriter) HandleAfter(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {
	stmt := gcg.Statements()
	stmt.Tab().Token("if err == nil {").Line()
	stmt.Tab().Tab().Token("if cmtErr := mysql.Commit(ctx); cmtErr != nil {").Line()
	stmt.Tab().Tab().Tab().Token("err = cmtErr").Line()
	stmt.Tab().Tab().Tab().Token("return").Line()
	stmt.Tab().Tab().Token("}").Line()
	stmt.Tab().Token("} else {").Line()
	stmt.Tab().Tab().Token("mysql.Rollback(ctx)").Line()
	stmt.Tab().Token("}").Line()

	code = stmt
	return
}

func (writer *TransactionWriter) ProxyBefore(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {

	return
}

func (writer *TransactionWriter) ProxyAfter(ctx context.Context, params []string, hasFnParam bool, hasFnResult bool) (code gcg.Code, err error) {

	return
}
