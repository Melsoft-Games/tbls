package md

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/gobuffalo/packr/v2"
	"github.com/Melsoft-Games/tbls/config"
	"github.com/Melsoft-Games/tbls/schema"
	"github.com/mattn/go-runewidth"
	"github.com/pkg/errors"
	"github.com/pmezard/go-difflib/difflib"
)

// Md struct
type Md struct {
	config *config.Config
	er     bool
	box    *packr.Box
}

// NewMd return Md
func NewMd(c *config.Config, er bool) *Md {
	return &Md{
		config: c,
		er:     er,
		box:    packr.New("md", "./templates"),
	}
}

// OutputSchema output .md format for all tables.
func (m *Md) OutputSchema(wr io.Writer, s *schema.Schema) error {
	ts, err := m.box.FindString("index.md.tmpl")
	if err != nil {
		return errors.WithStack(err)
	}
	tmpl := template.Must(template.New("index").Funcs(funcMap()).Parse(ts))
	templateData := makeSchemaTemplateData(s, m.config.Format.Adjust)
	templateData["er"] = m.er
	templateData["erFormat"] = m.config.ER.Format
	err = tmpl.Execute(wr, templateData)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// OutputTable output md format for table.
func (m *Md) OutputTable(wr io.Writer, t *schema.Table) error {
	ts, err := m.box.FindString("table.md.tmpl")
	if err != nil {
		return errors.WithStack(err)
	}
	tmpl := template.Must(template.New(t.Name).Funcs(funcMap()).Parse(ts))
	templateData := makeTableTemplateData(t, m.config.Format.Adjust)
	templateData["er"] = m.er
	templateData["erFormat"] = m.config.ER.Format

	err = tmpl.Execute(wr, templateData)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// Output generate markdown files.
func Output(s *schema.Schema, c *config.Config, force bool) error {
	docPath := c.DocPath

	fullPath, err := filepath.Abs(docPath)
	if err != nil {
		return errors.WithStack(err)
	}

	if !force && outputExists(s, fullPath) {
		return errors.New("output files already exists")
	}

	err = os.MkdirAll(fullPath, 0755) // #nosec
	if err != nil {
		return errors.WithStack(err)
	}

	// README.md
	file, err := os.Create(filepath.Join(fullPath, "README.md"))
	defer file.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	er := false
	if _, err := os.Lstat(filepath.Join(fullPath, fmt.Sprintf("schema.%s", c.ER.Format))); err == nil {
		er = true
	}

	md := NewMd(c, er)

	err = md.OutputSchema(file, s)
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Printf("%s\n", filepath.Join(docPath, "README.md"))

	// tables
	for _, t := range s.Tables {
		file, err := os.Create(filepath.Join(fullPath, fmt.Sprintf("%s.md", t.Name)))
		if err != nil {
			_ = file.Close()
			return errors.WithStack(err)
		}

		er := false
		if _, err := os.Lstat(filepath.Join(fullPath, fmt.Sprintf("%s.%s", t.Name, c.ER.Format))); err == nil {
			er = true
		}

		md := NewMd(c, er)

		err = md.OutputTable(file, t)
		if err != nil {
			_ = file.Close()
			return errors.WithStack(err)
		}
		fmt.Printf("%s\n", filepath.Join(docPath, fmt.Sprintf("%s.md", t.Name)))
		err = file.Close()
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// Diff database and markdown files.
func Diff(s *schema.Schema, c *config.Config) (string, error) {
	docPath := c.DocPath

	var diff string
	fullPath, err := filepath.Abs(docPath)
	if err != nil {
		return "", errors.WithStack(err)
	}

	if !outputExists(s, fullPath) {
		return "", errors.New("target files does not exists")
	}

	// README.md
	a := new(bytes.Buffer)
	er := false
	if _, err := os.Lstat(filepath.Join(fullPath, fmt.Sprintf("schema.%s", c.ER.Format))); err == nil {
		er = true
	}

	md := NewMd(c, er)

	err = md.OutputSchema(a, s)
	if err != nil {
		return "", errors.WithStack(err)
	}

	targetPath := filepath.Join(fullPath, "README.md")
	b, err := ioutil.ReadFile(filepath.Clean(targetPath))
	if err != nil {
		b = []byte{}
	}

	from, err := c.MaskedDSN()
	if err != nil {
		return "", errors.WithStack(err)
	}
	to := filepath.Join(docPath, "README.md")

	d := difflib.UnifiedDiff{
		A:        difflib.SplitLines(a.String()),
		B:        difflib.SplitLines(string(b)),
		FromFile: from,
		ToFile:   to,
		Context:  3,
	}

	text, _ := difflib.GetUnifiedDiffString(d)
	if text != "" {
		diff += fmt.Sprintf("diff %s %s\n", from, to)
		diff += text
	}

	// tables
	for _, t := range s.Tables {
		a := new(bytes.Buffer)
		er := false
		if _, err := os.Lstat(filepath.Join(fullPath, fmt.Sprintf("%s.%s", t.Name, c.ER.Format))); err == nil {
			er = true
		}

		md := NewMd(c, er)

		err := md.OutputTable(a, t)
		if err != nil {
			return "", errors.WithStack(err)
		}
		targetPath := filepath.Join(fullPath, fmt.Sprintf("%s.md", t.Name))
		b, err := ioutil.ReadFile(filepath.Clean(targetPath))
		if err != nil {
			b = []byte{}
		}

		to := filepath.Join(docPath, fmt.Sprintf("%s.md", t.Name))

		d := difflib.UnifiedDiff{
			A:        difflib.SplitLines(a.String()),
			B:        difflib.SplitLines(string(b)),
			FromFile: from,
			ToFile:   to,
			Context:  3,
		}

		text, _ := difflib.GetUnifiedDiffString(d)
		if text != "" {
			diff += fmt.Sprintf("diff %s %s\n", from, to)
			diff += text
		}
	}
	return diff, nil
}

func outputExists(s *schema.Schema, path string) bool {
	// README.md
	if _, err := os.Lstat(filepath.Join(path, "README.md")); err == nil {
		return true
	}
	// tables
	for _, t := range s.Tables {
		if _, err := os.Lstat(filepath.Join(path, fmt.Sprintf("%s.md", t.Name))); err == nil {
			return true
		}
	}
	return false
}

func funcMap() map[string]interface{} {
	return template.FuncMap{
		"nl2br": func(text string) string {
			r := strings.NewReplacer("\r\n", "<br>", "\n", "<br>", "\r", "<br>")
			return r.Replace(text)
		},
		"nl2mdnl": func(text string) string {
			r := strings.NewReplacer("\r\n", "  \n", "\n", "  \n", "\r", "  \n")
			return r.Replace(text)
		},
	}
}

func makeSchemaTemplateData(s *schema.Schema, adjust bool) map[string]interface{} {
	tablesData := [][]string{
		[]string{"Name", "Columns", "Comment", "Type"},
		[]string{"----", "-------", "-------", "----"},
	}
	for _, t := range s.Tables {
		data := []string{
			fmt.Sprintf("[%s](%s.md)", t.Name, t.Name),
			fmt.Sprintf("%d", len(t.Columns)),
			t.Comment,
			t.Type,
		}
		tablesData = append(tablesData, data)
	}

	if adjust {
		return map[string]interface{}{
			"Schema": s,
			"Tables": adjustTable(tablesData),
		}
	}

	return map[string]interface{}{
		"Schema": s,
		"Tables": tablesData,
	}
}

func makeTableTemplateData(t *schema.Table, adjust bool) map[string]interface{} {
	// Columns
	columnsData := [][]string{
		[]string{"Name", "Type", "Default", "Nullable", "Children", "Parents", "Comment"},
		[]string{"----", "----", "-------", "--------", "--------", "-------", "-------"},
	}
	for _, c := range t.Columns {
		childRelations := []string{}
		cEncountered := map[string]bool{}
		for _, r := range c.ChildRelations {
			if _, ok := cEncountered[r.Table.Name]; ok {
				continue
			}
			childRelations = append(childRelations, fmt.Sprintf("[%s](%s.md)", r.Table.Name, r.Table.Name))
			cEncountered[r.Table.Name] = true
		}
		parentRelations := []string{}
		pEncountered := map[string]bool{}
		for _, r := range c.ParentRelations {
			if _, ok := pEncountered[r.ParentTable.Name]; ok {
				continue
			}
			parentRelations = append(parentRelations, fmt.Sprintf("[%s](%s.md)", r.ParentTable.Name, r.ParentTable.Name))
			pEncountered[r.ParentTable.Name] = true
		}
		data := []string{
			c.Name,
			c.Type,
			c.Default.String,
			fmt.Sprintf("%v", c.Nullable),
			strings.Join(childRelations, " "),
			strings.Join(parentRelations, " "),
			c.Comment,
		}
		columnsData = append(columnsData, data)
	}

	// Constraints
	constraintsData := [][]string{
		[]string{"Name", "Type", "Definition"},
		[]string{"----", "----", "----------"},
	}
	for _, c := range t.Constraints {
		data := []string{
			c.Name,
			c.Type,
			c.Def,
		}
		constraintsData = append(constraintsData, data)
	}

	// Indexes
	indexesData := [][]string{
		[]string{"Name", "Definition"},
		[]string{"----", "----------"},
	}
	for _, i := range t.Indexes {
		data := []string{
			i.Name,
			i.Def,
		}
		indexesData = append(indexesData, data)
	}

	// Triggers
	triggersData := [][]string{
		[]string{"Name", "Definition"},
		[]string{"----", "----------"},
	}
	for _, i := range t.Triggers {
		data := []string{
			i.Name,
			i.Def,
		}
		triggersData = append(triggersData, data)
	}

	if adjust {
		return map[string]interface{}{
			"Table":       t,
			"Columns":     adjustTable(columnsData),
			"Constraints": adjustTable(constraintsData),
			"Indexes":     adjustTable(indexesData),
			"Triggers":    adjustTable(triggersData),
		}
	}

	return map[string]interface{}{
		"Table":       t,
		"Columns":     columnsData,
		"Constraints": constraintsData,
		"Indexes":     indexesData,
		"Triggers":    triggersData,
	}
}

func adjustTable(data [][]string) [][]string {
	r := strings.NewReplacer("\r\n", "<br>", "\n", "<br>", "\r", "<br>")
	w := make([]int, len(data[0]))
	for i := range data {
		for j := range w {
			l := runewidth.StringWidth(r.Replace(data[i][j]))
			if l > w[j] {
				w[j] = l
			}
		}
	}
	for i := range data {
		for j := range w {
			if i == 1 {
				data[i][j] = strings.Repeat("-", w[j])
			} else {
				data[i][j] = fmt.Sprintf(fmt.Sprintf("%%-%ds", w[j]), r.Replace(data[i][j]))
			}
		}
	}

	return data
}
