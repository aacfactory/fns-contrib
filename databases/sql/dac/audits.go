package dac

import (
	"github.com/aacfactory/fns/commons/uid"
	"time"
)

func NewAuditCreation[Id ~string | ~int64](id Id) AuditCreation[Id] {
	return AuditCreation[Id]{
		CreateBY: id,
		CreateAT: time.Now(),
	}
}

type AuditCreation[Id ~string | ~int64] struct {
	CreateBY Id        `column:"CREATE_BY,ACB" json:"createBY"`
	CreateAT time.Time `column:"CREATE_AT,ACT" json:"createAT"`
}

func NewAuditModification[Id ~string | ~int64](id Id) AuditModification[Id] {
	return AuditModification[Id]{
		ModifyBY: id,
		ModifyAT: time.Now(),
	}
}

type AuditModification[Id ~string | ~int64] struct {
	ModifyBY Id        `column:"MODIFY_BY,AMB" json:"modifyBY"`
	ModifyAT time.Time `column:"MODIFY_AT,AMT" json:"modifyAT"`
}

func NewAuditDeletion[Id ~string | ~int64](id Id) AuditDeletion[Id] {
	return AuditDeletion[Id]{
		DeleteBY: id,
		DeleteAT: time.Now(),
	}
}

type AuditDeletion[Id ~string | ~int64] struct {
	DeleteBY Id        `column:"DELETE_BY,ADB" json:"deleteBY"`
	DeleteAT time.Time `column:"DELETE_AT,ADT" json:"deleteAT"`
}

type AuditVersion struct {
	Version int64 `column:"VERSION,AOL" json:"version"`
}

type SID struct {
	Id int64 `column:"ID,PK,INCR" json:"id"`
}

func NewUID() UID {
	return UID{
		Id: uid.UID(),
	}
}

type UID struct {
	Id string `column:"ID,PK" json:"id"`
}

func NewAudit(id string) Audit {
	return Audit{
		UID: UID{
			Id: id,
		},
		AuditCreation:     AuditCreation[string]{},
		AuditModification: AuditModification[string]{},
		AuditDeletion:     AuditDeletion[string]{},
		AuditVersion:      AuditVersion{},
	}
}

type Audit struct {
	UID
	AuditCreation[string]
	AuditModification[string]
	AuditDeletion[string]
	AuditVersion
}

func (audit Audit) WithCreation(id string) Audit {
	audit.CreateBY = id
	audit.CreateAT = time.Now()
	return audit
}

func (audit Audit) WithModification(id string) Audit {
	audit.ModifyBY = id
	audit.ModifyAT = time.Now()
	return audit
}

func (audit Audit) WithDeletion(id string) Audit {
	audit.DeleteBY = id
	audit.DeleteAT = time.Now()
	return audit
}

func NewAuditWithoutDeletion(id string) AuditWithoutDeletion {
	return AuditWithoutDeletion{
		UID: UID{
			Id: id,
		},
		AuditCreation:     AuditCreation[string]{},
		AuditModification: AuditModification[string]{},
		AuditVersion:      AuditVersion{},
	}
}

type AuditWithoutDeletion struct {
	UID
	AuditCreation[string]
	AuditModification[string]
	AuditVersion
}

func (audit AuditWithoutDeletion) WithCreation(id string) AuditWithoutDeletion {
	audit.CreateBY = id
	audit.CreateAT = time.Now()
	return audit
}

func (audit AuditWithoutDeletion) WithModification(id string) AuditWithoutDeletion {
	audit.ModifyBY = id
	audit.ModifyAT = time.Now()
	return audit
}

func NewAuditWithoutModificationAndDeletion(id string) AuditWithoutModificationAndDeletion {
	return AuditWithoutModificationAndDeletion{
		UID: UID{
			Id: id,
		},
		AuditCreation: AuditCreation[string]{},
	}
}

type AuditWithoutModificationAndDeletion struct {
	UID
	AuditCreation[string]
}

func (audit AuditWithoutModificationAndDeletion) WithCreation(id string) AuditWithoutModificationAndDeletion {
	audit.CreateBY = id
	audit.CreateAT = time.Now()
	return audit
}

func NewAuditWithIncrPk(id int64) IncrPkAudit {
	return IncrPkAudit{
		SID: SID{
			Id: id,
		},
		AuditCreation:     AuditCreation[int64]{},
		AuditModification: AuditModification[int64]{},
		AuditDeletion:     AuditDeletion[int64]{},
		AuditVersion:      AuditVersion{},
	}
}

type IncrPkAudit struct {
	SID
	AuditCreation[int64]
	AuditModification[int64]
	AuditDeletion[int64]
	AuditVersion
}

func (audit IncrPkAudit) WithCreation(id int64) IncrPkAudit {
	audit.CreateBY = id
	audit.CreateAT = time.Now()
	return audit
}

func (audit IncrPkAudit) WithModification(id int64) IncrPkAudit {
	audit.ModifyBY = id
	audit.ModifyAT = time.Now()
	return audit
}

func (audit IncrPkAudit) WithDeletion(id int64) IncrPkAudit {
	audit.DeleteBY = id
	audit.DeleteAT = time.Now()
	return audit
}

func NewIncrPkAuditWithoutDeletion(id int64) IncrPkAuditWithoutDeletion {
	return IncrPkAuditWithoutDeletion{
		SID: SID{
			Id: id,
		},
		AuditCreation:     AuditCreation[int64]{},
		AuditModification: AuditModification[int64]{},
		AuditVersion:      AuditVersion{},
	}
}

type IncrPkAuditWithoutDeletion struct {
	SID
	AuditCreation[int64]
	AuditModification[int64]
	AuditVersion
}

func (audit IncrPkAuditWithoutDeletion) WithCreation(id int64) IncrPkAuditWithoutDeletion {
	audit.CreateBY = id
	audit.CreateAT = time.Now()
	return audit
}

func (audit IncrPkAuditWithoutDeletion) WithModification(id int64) IncrPkAuditWithoutDeletion {
	audit.ModifyBY = id
	audit.ModifyAT = time.Now()
	return audit
}

func NewIncrPkAuditWithoutModificationAndDeletion(id int64) IncrPkAuditWithoutModificationAndDeletion {
	return IncrPkAuditWithoutModificationAndDeletion{
		SID: SID{
			Id: id,
		},
		AuditCreation: AuditCreation[int64]{},
	}
}

type IncrPkAuditWithoutModificationAndDeletion struct {
	SID
	AuditCreation[int64]
}

func (audit IncrPkAuditWithoutModificationAndDeletion) WithCreation(id int64) IncrPkAuditWithoutModificationAndDeletion {
	audit.CreateBY = id
	audit.CreateAT = time.Now()
	return audit
}
