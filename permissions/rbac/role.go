package rbac

import (
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/commons/wildcard"
	"sort"
)

type Policy struct {
	Object string `json:"object"`
	Action string `json:"action"`
}

func (p *Policy) match(action string) (ok bool) {
	ok = wildcard.Match(bytex.FromString(p.Action), bytex.FromString(action))
	return
}

type Roles []Role

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

func (roles Roles) Add(role Role) (v Roles) {
	for _, r := range roles {
		if r.Mount(role) {
			v = roles
			return
		}
	}
	v = append(roles, role)
	sort.Sort(v)
	return
}

func (roles Roles) Get(id string) (role Role, has bool) {
	for _, r := range roles {
		if r.Id == id {
			role = r
			has = true
			return
		}
		if len(r.Children) > 0 {
			role, has = r.Children.Get(id)
			if has {
				return
			}
		}
	}
	return
}

func (roles Roles) CheckPolicy(object string, action string) (ok bool) {
	if roles == nil || len(roles) == 0 {
		return
	}
	for _, role := range roles {
		if role.CheckPolicy(object, action) {
			ok = true
			return
		}
	}
	return
}

func (roles Roles) Remove(role Role) (v Roles) {
	for _, r := range roles {
		if r.Id == role.Id {
			continue
		}
		if len(r.Children) > 0 {
			r.Children = r.Children.Remove(role)
		}
		v = append(v, r)
	}
	return
}

type Role struct {
	Id          string   `json:"id" tree:"ParentId+Children"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	ParentId    string   `json:"parentId"`
	Children    Roles    `json:"children"`
	Policies    []Policy `json:"policies"`
}

func (role *Role) CheckPolicy(object string, action string) (ok bool) {
	if role.Policies != nil && len(role.Policies) > 0 {
		for _, policy := range role.Policies {
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

func (role *Role) Ids() (ids []string) {
	ids = append(ids, role.Id)
	for _, child := range role.Children {
		ids = append(ids, child.Ids()...)
	}
	return
}

func (role *Role) Contains(target Role) (ok bool) {
	if role.Id == target.Id {
		ok = true
		return
	}
	for _, child := range role.Children {
		if child.Contains(target) {
			ok = true
			return
		}
	}
	return
}

func (role *Role) Mount(target Role) (ok bool) {
	if role.Id == target.ParentId {
		for i, child := range role.Children {
			if child.Id == target.Id {
				role.Children[i] = target
				ok = true
				return
			}
		}
		role.Children = append(role.Children, target)
		sort.Sort(role.Children)
		ok = true
		return
	}
	for _, child := range role.Children {
		if child.Mount(target) {
			ok = true
			return
		}
	}
	return
}
