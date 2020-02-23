package mysql

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/Melsoft-Games/tbls/schema"
	"github.com/xo/dburl"
)

var s *schema.Schema
var db *sql.DB

func TestMain(m *testing.M) {
	s = &schema.Schema{
		Name: "testdb",
	}
	db, _ = dburl.Open("my://root:mypass@localhost:33306/testdb")
	defer db.Close()
	exit := m.Run()
	if exit != 0 {
		os.Exit(exit)
	}
}

func TestAnalyzeView(t *testing.T) {
	driver := NewMysql(db)
	err := driver.Analyze(s)
	if err != nil {
		t.Errorf("%v", err)
	}
	view, _ := s.FindTableByName("post_comments")
	expected := view.Def
	if expected == "" {
		t.Errorf("actual not empty string.")
	}
}

func TestInfo(t *testing.T) {
	driver := NewMysql(db)
	d, err := driver.Info()
	if err != nil {
		t.Errorf("%v", err)
	}
	if d.Name != "mysql" {
		t.Errorf("actual %v\nwant %v", d.Name, "mysql")
	}
	if d.DatabaseVersion == "" {
		t.Errorf("actual not empty string.")
	}
}
