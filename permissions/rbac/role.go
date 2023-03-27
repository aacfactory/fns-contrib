package rbac

import "github.com/aacfactory/fns/commons/wildcard"

type Policy struct {
	Object string `json:"object"`
	Action string `json:"action"`
}

func (p *Policy) match(action string) (ok bool) {
	ok = wildcard.Match(p.Action, action)
	return
}

type Roles []*Role

func (roles Roles) Len() int {
	return len(roles)
}

func (roles Roles) Less(i, j int) bool {
	return roles[i].Id < roles[j].Id
}

func (roles Roles) Swap(i, j int) {
	roles[i], roles[j] = roles[j], roles[i]
	return
}

func (roles Roles) CheckPolicy(object string, action string) (ok bool) {
	if roles == nil || len(roles) == 0 {
		return
	}
	for _, role := range roles {
		if role == nil {
			continue
		}
		if role.CheckPolicy(object, action) {
			ok = true
			return
		}
	}
	return
}

type Role struct {
	Id          string    `json:"id" tree:"ParentId+Children"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	ParentId    string    `json:"parentId"`
	Children    Roles     `json:"children"`
	Policies    []*Policy `json:"policies"`
}

func (role *Role) CheckPolicy(object string, action string) (ok bool) {
	if role.Policies != nil && len(role.Policies) > 0 {
		for _, policy := range role.Policies {
			if policy == nil {
				continue
			}
			if policy.Object == object && policy.match(action) {
				ok = true
				return
			}
		}
	}
	if role.Children != nil && len(role.Children) > 0 {
		for _, child := range role.Children {
			ok = child.CheckPolicy(object, action)
			if ok {
				return
			}
		}
	}
	return
}
