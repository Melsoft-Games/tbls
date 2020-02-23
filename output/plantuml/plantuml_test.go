package plantuml

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/Melsoft-Games/tbls/config"
	"github.com/Melsoft-Games/tbls/schema"
)

func TestOutputSchema(t *testing.T) {
	s := newTestSchema()
	c, err := config.NewConfig()
	if err != nil {
		t.Error(err)
	}
	err = c.LoadConfigFile(filepath.Join(testdataDir(), "out_test_tbls.yml"))
	if err != nil {
		t.Error(err)
	}
	err = c.MergeAdditionalData(s)
	if err != nil {
		t.Error(err)
	}
	o := NewPlantUML(c)
	buf := &bytes.Buffer{}
	err = o.OutputSchema(buf, s)
	if err != nil {
		t.Error(err)
	}
	expected, _ := ioutil.ReadFile(filepath.Join(testdataDir(), "plantuml_test_schema.puml.golden"))
	actual := buf.String()
	if actual != string(expected) {
		t.Errorf("actual %v\nwant %v", actual, string(expected))
	}
}

func TestOutputTable(t *testing.T) {
	s := newTestSchema()
	c, err := config.NewConfig()
	if err != nil {
		t.Error(err)
	}
	err = c.LoadConfigFile(filepath.Join(testdataDir(), "out_test_tbls.yml"))
	if err != nil {
		t.Error(err)
	}
	err = c.MergeAdditionalData(s)
	if err != nil {
		t.Error(err)
	}
	ta := s.Tables[0]

	o := NewPlantUML(c)
	buf := &bytes.Buffer{}
	_ = o.OutputTable(buf, ta)
	expected, _ := ioutil.ReadFile(filepath.Join(testdataDir(), "plantuml_test_a.puml.golden"))
	actual := buf.String()
	if actual != string(expected) {
		t.Errorf("actual %v\nwant %v", actual, string(expected))
	}
}

func testdataDir() string {
	wd, _ := os.Getwd()
	dir, _ := filepath.Abs(filepath.Join(filepath.Dir(filepath.Dir(wd)), "testdata"))
	return dir
}

func newTestSchema() *schema.Schema {
	ca := &schema.Column{
		Name:    "a",
		Comment: "column a",
	}
	cb := &schema.Column{
		Name:    "b",
		Comment: "column b",
	}

	ta := &schema.Table{
		Name:    "a",
		Comment: "table a",
		Columns: []*schema.Column{
			ca,
			&schema.Column{
				Name:    "a2",
				Comment: "column a2",
			},
		},
	}
	ta.Indexes = []*schema.Index{
		&schema.Index{
			Name:    "PRIMARY KEY",
			Def:     "PRIMARY KEY(a)",
			Table:   &ta.Name,
			Columns: []string{"a"},
		},
	}
	tb := &schema.Table{
		Name:    "b",
		Comment: "table b",
		Columns: []*schema.Column{
			cb,
			&schema.Column{
				Name:    "b2",
				Comment: "column b2",
			},
		},
	}
	r := &schema.Relation{
		Table:         tb,
		Columns:       []*schema.Column{cb},
		ParentTable:   ta,
		ParentColumns: []*schema.Column{ca},
	}
	ca.ChildRelations = []*schema.Relation{r}
	cb.ParentRelations = []*schema.Relation{r}

	s := &schema.Schema{
		Name: "testschema",
		Tables: []*schema.Table{
			ta,
			tb,
		},
		Relations: []*schema.Relation{
			r,
		},
		Driver: &schema.Driver{
			Name:            "testdriver",
			DatabaseVersion: "1.0.0",
		},
	}
	return s
}
