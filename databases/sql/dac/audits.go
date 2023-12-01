package dac

import "time"

type AuditCreation[Id ~string | ~int64] struct {
	CreateBY Id        `column:"CREATE_BY,ACB" json:"createBY"`
	CreateAT time.Time `column:"CREATE_AT,ACT" json:"createAT"`
}

type AuditModification[Id ~string | ~int64] struct {
	ModifyBY Id        `column:"MODIFY_BY,AMB" json:"modifyBY"`
	ModifyAT time.Time `column:"MODIFY_AT,AMT" json:"modifyAT"`
}

type AuditDelete[Id ~string | ~int64] struct {
	DeleteBY Id        `column:"DELETE_BY,ADB" json:"deleteBY"`
	DeleteAT time.Time `column:"DELETE_AT,ADT" json:"deleteAT"`
}

type AuditVersion struct {
	Version int64 `column:"VERSION,AOL" json:"version"`
}

type SID struct {
	Id int64 `column:"ID,PK,INCR" json:"id"`
}

type UID struct {
	Id string `column:"ID,PK" json:"id"`
}

type Audit struct {
	UID
	AuditCreation[string]
	AuditModification[string]
	AuditDelete[string]
	AuditVersion
}

type AuditWithIncrPk struct {
	SID
	AuditCreation[int64]
	AuditModification[int64]
	AuditDelete[int64]
	AuditVersion
}
