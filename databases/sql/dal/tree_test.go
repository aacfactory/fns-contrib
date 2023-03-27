package dal_test

import (
	"encoding/json"
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/sql/dal"
	"testing"
)

type Departments []*Department

func (departments Departments) Len() int {
	return len(departments)
}

func (departments Departments) Less(i, j int) bool {
	return departments[i].Id < departments[j].Id
}

func (departments Departments) Swap(i, j int) {
	departments[i], departments[j] = departments[j], departments[i]
	return
}

type Department struct {
	Id       string      `col:"ID,pk"`
	ParentId string      `col:"PARENT_ID"`
	Children Departments `col:"CHILDREN,TREE,ID+PARENT_ID"`
}

func (department *Department) TableName() (schema string, name string) {
	schema = "FNS"
	name = "DEPARTMENT"
	return
}

func (department *Department) String() (s string) {
	p, _ := json.MarshalIndent(department, "\t", "\t")
	s = string(p)
	return
}

func TestMapListToTree(t *testing.T) {
	list := []*Department{
		{
			Id: "A", ParentId: "",
		},
		{
			Id: "A2", ParentId: "A",
		},
		{
			Id: "A1", ParentId: "A",
		},
		{
			Id: "A12", ParentId: "A1",
		},
		{
			Id: "A11", ParentId: "A1",
		},
	}

	nodes, mapErr := dal.MapListToTrees[*Department, string](list, nil)
	if mapErr != nil {
		t.Errorf("%+v", mapErr)
		return
	}
	for _, node := range nodes {
		fmt.Println(node.String())
		fmt.Println("----")
	}

}
