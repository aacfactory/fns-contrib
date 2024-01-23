package spec

import (
	"cmp"
	"github.com/aacfactory/fns/commons/versions"
	"github.com/aacfactory/fns/services/documents"
	"sort"
)

type Documents []Document

func (doc Documents) Len() int {
	return len(doc)
}

func (doc Documents) Less(i, j int) bool {
	return doc[j].Version.LessThan(doc[i].Version)
}

func (doc Documents) Swap(i, j int) {
	doc[i], doc[j] = doc[j], doc[i]
	return
}

func (doc Documents) Add(endpoint documents.Endpoint) Documents {
	for i, document := range doc {
		if document.Version.Equals(endpoint.Version) {
			document.Endpoints = append(document.Endpoints, endpoint)
			sort.Sort(document.Endpoints)
			doc[i] = document
			return doc
		}
	}
	return append(doc, Document{
		Version:   endpoint.Version,
		Endpoints: Endpoints{endpoint},
	})
}

func (doc Documents) Match(ver versions.Version) (v Document) {
	matched := make(Endpoints, 0)
	for _, document := range doc {
		ok := false
		if document.Version.Major == ver.Major {
			if ver.Minor == -1 {
				ok = true
			} else if document.Version.Minor == ver.Minor {
				if ver.Patch == -1 {
					ok = true
				} else if document.Version.Patch == ver.Patch {
					ok = true
				}
			}
		}
		if ok {
			for _, endpoint := range document.Endpoints {
				exist := false
				for i, m := range matched {
					if m.Name == endpoint.Name {
						if m.Version.LessThan(endpoint.Version) {
							matched[i] = endpoint
						}
						exist = true
						break
					}
				}
				if exist {
					continue
				}
				matched = append(matched, endpoint)
			}
		}
	}
	sort.Sort(matched)
	v = Document{
		Version:   ver,
		Endpoints: matched,
	}
	return
}

func (doc Documents) Latest() (v Document) {
	matched := make(Endpoints, 0)
	for _, document := range doc {
		for _, endpoint := range document.Endpoints {
			exist := false
			for i, m := range matched {
				if m.Version.LessThan(endpoint.Version) {
					matched[i] = endpoint
				}
				exist = true
				break
			}
			if exist {
				continue
			}
			matched = append(matched, endpoint)
		}
	}
	sort.Sort(matched)
	v = Document{
		Version:   versions.Latest(),
		Endpoints: matched,
	}
	return
}

type Endpoints []documents.Endpoint

func (endpoints Endpoints) Len() int {
	return len(endpoints)
}

func (endpoints Endpoints) Less(i, j int) bool {
	return endpoints[i].Name < endpoints[j].Name
}

func (endpoints Endpoints) Swap(i, j int) {
	endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
	return
}

func (endpoints Endpoints) Contains(endpoint documents.Endpoint) (idx int, ok bool) {
	size := len(endpoints)
	if size == 0 {
		return
	}
	idx, ok = sort.Find(size, func(i int) int {
		return cmp.Compare[string](endpoint.Name, endpoints[i].Name)
	})
	return
}

type Document struct {
	Version   versions.Version
	Endpoints Endpoints
}
