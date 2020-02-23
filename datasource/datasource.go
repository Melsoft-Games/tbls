package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/Melsoft-Games/tbls/drivers"
	"github.com/Melsoft-Games/tbls/drivers/bq"
	"github.com/Melsoft-Games/tbls/drivers/mysql"
	"github.com/Melsoft-Games/tbls/drivers/postgres"
	"github.com/Melsoft-Games/tbls/schema"
	"github.com/pkg/errors"
	"github.com/xo/dburl"
)

func Analyze(dsn []string) (*schema.Schema, error) {
	s := &schema.Schema{}
	for _, d := range dsn {
		if err := AnalyzeImpl(d, s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Analyze database
func AnalyzeImpl(urlstr string, s *schema.Schema) error {
	if strings.Index(urlstr, "json://") == 0 {
		return AnalizeJSON(urlstr, s)
	}
	if strings.Index(urlstr, "bq://") == 0 || strings.Index(urlstr, "bigquery://") == 0 {
		return AnalizeBigquery(urlstr, s)
	}
	u, err := dburl.Parse(urlstr)
	if err != nil {
		return errors.WithStack(err)
	}
	splitted := strings.Split(u.Short(), "/")
	if len(splitted) < 2 {
		return errors.WithStack(fmt.Errorf("invalid DSN: parse %s -> %#v", urlstr, u))
	}

	db, err := dburl.Open(urlstr)
	defer db.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	if err = db.Ping(); err != nil {
		return errors.WithStack(err)
	}

	var driver drivers.Driver

	switch u.Driver {
	case "postgres":
		s.Name = "Postgres schema"
		driver = postgres.NewPostgres(db)
	case "mysql":
		s.Name = "MySQL schema"
		driver = mysql.NewMysql(db)
	default:
		return errors.WithStack(fmt.Errorf("unsupported driver '%s'", u.Driver))
	}
	d, err := driver.Info()
	if err != nil {
		return err
	}
	s.Driver = d
	err = driver.Analyze(s)
	if err != nil {
		return err
	}
	return nil
}

// AnalizeJSON analyze `json://`
func AnalizeJSON(urlstr string, s *schema.Schema) error {
	splitted := strings.Split(urlstr, "json://")
	file, err := os.Open(splitted[1])
	if err != nil {
		return errors.WithStack(err)
	}
	dec := json.NewDecoder(file)
	err = dec.Decode(s)
	if err != nil {
		return errors.WithStack(err)
	}
	err = s.Repair()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// AnalizeBigquery analyze `bq://`
func AnalizeBigquery(urlstr string, s *schema.Schema) error {
	u, err := url.Parse(urlstr)
	if err != nil {
		return err
	}

	values := u.Query()
	err = setEnvGoogleApplicationCredentials(values)
	if err != nil {
		return err
	}

	splitted := strings.Split(u.Path, "/")

	projectID := u.Host
	datasetID := splitted[1]

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	s.Name = "BQ Schema"
	driver, err := bq.NewBigquery(ctx, client, datasetID)
	if err != nil {
		return err
	}
	d, err := driver.Info()
	if err != nil {
		return err
	}
	s.Driver = d
	err = driver.Analyze(s)
	if err != nil {
		return err
	}
	return nil
}

func setEnvGoogleApplicationCredentials(values url.Values) error {
	keys := []string{
		"google_application_credentials",
		"credentials",
		"creds",
	}
	for _, k := range keys {
		if values.Get(k) != "" {
			return os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", values.Get(k))
		}
	}
	return nil
}
