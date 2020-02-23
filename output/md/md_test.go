package md

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/Melsoft-Games/tbls/config"
	"github.com/Melsoft-Games/tbls/schema"
)

var tests = []struct {
	name         string
	actualFile   string
	expectedFile string
	adjust       bool
}{
	{"README.md", "README.md", "md_test_README.md.golden", false},
	{"a.md", "a.md", "md_test_a.md.golden", false},
	{"--adjust option", "README.md", "md_test_README.md.adjust.golden", true},
}

func TestOutput(t *testing.T) {
	for _, tt := range tests {
		s := newTestSchema()
		c, err := config.NewConfig()
		if err != nil {
			t.Error(err)
		}
		tempDir, _ := ioutil.TempDir("", "tbls")
		force := true
		adjust := tt.adjust
		erFormat := "png"
		defer os.RemoveAll(tempDir)
		err = c.Load(filepath.Join(testdataDir(), "out_test_tbls.yml"), config.DocPath(tempDir), config.Adjust(adjust), config.ERFormat(erFormat))
		if err != nil {
			t.Error(err)
		}
		err = c.MergeAdditionalData(s)
		if err != nil {
			t.Error(err)
		}
		err = Output(s, c, force)
		if err != nil {
			t.Error(err)
		}
		expected, err := ioutil.ReadFile(filepath.Join(testdataDir(), tt.expectedFile))
		if err != nil {
			t.Error(err)
		}
		actual, err := ioutil.ReadFile(filepath.Join(tempDir, tt.actualFile))
		if err != nil {
			log.Fatal(err)
		}
		if string(actual) != string(expected) {
			t.Errorf("actual %v\nwant %v", string(actual), string(expected))
		}
	}
}

func TestDiff(t *testing.T) {
	for _, tt := range tests {
		s := newTestSchema()
		c, err := config.NewConfig()
		if err != nil {
			t.Error(err)
		}
		tempDir, _ := ioutil.TempDir("", "tbls")
		force := true
		adjust := tt.adjust
		erFormat := "png"
		defer os.RemoveAll(tempDir)
		err = c.Load(filepath.Join(testdataDir(), "out_test_tbls.yml"), config.DocPath(tempDir), config.Adjust(adjust), config.ERFormat(erFormat))
		if err != nil {
			t.Error(err)
		}
		err = c.MergeAdditionalData(s)
		if err != nil {
			t.Error(err)
		}
		err = Output(s, c, force)
		if err != nil {
			t.Error(err)
		}
		expected := ""
		actual, err := Diff(s, c)
		if err != nil {
			t.Error(err)
		}
		if actual != expected {
			t.Errorf("actual %v\nwant %v", actual, expected)
		}
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
		Table:         ta,
		Columns:       []*schema.Column{ca},
		ParentTable:   tb,
		ParentColumns: []*schema.Column{cb},
	}
	ca.ParentRelations = []*schema.Relation{r}
	cb.ChildRelations = []*schema.Relation{r}

	s := &schema.Schema{
		Name: "testschema",
		Tables: []*schema.Table{
			ta,
			tb,
		},
		Relations: []*schema.Relation{
			r,
		},
	}
	return s
}
