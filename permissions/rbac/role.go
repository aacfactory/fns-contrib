package rbac

import "github.com/aacfactory/fns/commons/wildcard"

type Policy struct {
	Object  string `json:"object"`
	Action  string `json:"action"`
	matcher *wildcard.Wildcard
}

func (p *Policy) match(action string) (ok bool) {
	if p.matcher == nil {
		ok = true
		return
	}
	ok = p.matcher.Match(action)
	return
}

type Role struct {
	Id          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Parent      string    `json:"parent"`
	Children    []*Role   `json:"children"`
	Policies    []*Policy `json:"policies"`
}
