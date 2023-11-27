package dac

import "time"

type AuditCreation[Id ~string | ~int64] struct {
	CreateBY Id        `column:"CREATE_BY,ACB"`
	CreateAT time.Time `column:"CREATE_AT,ACT"`
}

type AuditModification[Id ~string | ~int64] struct {
	ModifyBY Id        `column:"MODIFY_BY,AMB"`
	ModifyAT time.Time `column:"MODIFY_AT,AMT"`
}

type AuditDelete[Id ~string | ~int64] struct {
	DeleteBY Id        `column:"DELETE_BY,ADB"`
	DeleteAT time.Time `column:"DELETE_AT,ADT"`
}

type AuditVersion struct {
	Version int64 `column:"VERSION,AOL"`
}

type IncrPk struct {
	Id int64 `column:"ID,PK,INCR"`
}

type UID struct {
	Id string `column:"ID,PK"`
}

type Audit struct {
	UID
	AuditCreation[string]
	AuditModification[string]
	AuditDelete[string]
	AuditVersion
}

type AuditWithIncrPk struct {
	IncrPk
	AuditCreation[int64]
	AuditModification[int64]
	AuditDelete[int64]
	AuditVersion
}
