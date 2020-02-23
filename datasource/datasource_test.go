package datasource

import (
	"os"
	"path/filepath"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var tests = []struct {
	dsn           []string
	schemaName    string
	tableCount    int
	relationCount int
}{
	{[]string{"my://root:mypass@localhost:33306/testdb"}, "MySQL schema", 9, 6},
	{[]string{"pg://postgres:pgpass@localhost:55432/testdb?sslmode=disable"}, "Postgres schema", 11, 8},
	{[]string{"json://../testdata/testdb.json"}, "testdb", 7, 9},
}

func TestMain(m *testing.M) {
	cPath := credentialPath()
	if _, err := os.Lstat(cPath); err == nil {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", cPath)
		bqTest := struct {
			dsn           []string
			schemaName    string
			tableCount    int
			relationCount int
		}{
			[]string{"bq://bigquery-public-data/bitcoin_blockchain"}, "bigquery-public-data:bitcoin_blockchain", 2, 0,
		}
		tests = append(tests, bqTest)
	}
	exit := m.Run()
	if exit != 0 {
		os.Exit(exit)
	}
}

func TestAnalyzeSchema(t *testing.T) {
	for _, tt := range tests {
		schema, err := Analyze(tt.dsn)
		if err != nil {
			t.Errorf("%s", err)
		}
		expected := tt.schemaName
		actual := schema.Name
		if actual != expected {
			t.Errorf("actual %v\nwant %v", actual, expected)
		}
	}
}

func TestAnalyzeTables(t *testing.T) {
	for _, tt := range tests {
		schema, err := Analyze(tt.dsn)
		if err != nil {
			t.Errorf("%s", err)
		}
		expected := tt.tableCount
		actual := len(schema.Tables)
		if actual != expected {
			t.Errorf("actual %v\nwant %v", actual, expected)
		}
	}
}

func TestAnalyzeRelations(t *testing.T) {
	for _, tt := range tests {
		schema, err := Analyze(tt.dsn)
		if err != nil {
			t.Errorf("%s", err)
		}
		expected := tt.relationCount
		actual := len(schema.Relations)
		if actual != expected {
			t.Errorf("actual %v\nwant %v", actual, expected)
		}
	}
}

func credentialPath() string {
	wd, _ := os.Getwd()
	return filepath.Join(filepath.Dir(wd), "client_secrets.json")
}
