package config

import (
	"fmt"

	"github.com/k1LoW/tbls/schema"
)

// Lint is the struct for lint config
type Lint struct {
	RequireTableComment  RequireTableComment  `yaml:"requireTableComment"`
	RequireColumnComment RequireColumnComment `yaml:"requireColumnComment"`
	NoRelationTables     NoRelationTables     `yaml:"noRelationTables"`
	ColumnCount          ColumnCount          `yaml:"columnCount"`
}

// RuleWarn is struct of Rule error
type RuleWarn struct {
	Message string
}

// Rule is interfece of `tbls lint` cop
type Rule interface {
	IsEnabled() bool
	Check(*schema.Schema) []RuleWarn
}

// RequireTableComment check table comment
type RequireTableComment struct {
	Enabled bool `yaml:"enabled"`
}

// IsEnabled return Rule is enabled or not
func (l RequireTableComment) IsEnabled() bool {
	return l.Enabled
}

// Check table comment
func (l RequireTableComment) Check(s *schema.Schema) []RuleWarn {
	msgFmt := "%s: table comment required."
	warns := []RuleWarn{}
	for _, t := range s.Tables {
		if t.Comment == "" {
			warns = append(warns, RuleWarn{
				Message: fmt.Sprintf(msgFmt, t.Name),
			})
		}
	}
	return warns
}

// RequireColumnComment check column comment
type RequireColumnComment struct {
	Enabled bool `yaml:"enabled"`
}

// IsEnabled return Rule is enabled or not
func (l RequireColumnComment) IsEnabled() bool {
	return l.Enabled
}

// Check column comment
func (l RequireColumnComment) Check(s *schema.Schema) []RuleWarn {
	msgFmt := "%s.%s: column comment required."
	warns := []RuleWarn{}
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			if c.Comment == "" {
				warns = append(warns, RuleWarn{
					Message: fmt.Sprintf(msgFmt, t.Name, c.Name),
				})
			}
		}
	}
	return warns
}

// NoRelationTables check no relation table
type NoRelationTables struct {
	Enabled bool `yaml:"enabled"`
	Max     int  `yaml:"max"`
}

// IsEnabled return Rule is enabled or not
func (l NoRelationTables) IsEnabled() bool {
	return l.Enabled
}

// Check table relation
func (l NoRelationTables) Check(s *schema.Schema) []RuleWarn {
	msgFmt := "schema has too many no relation tables. [%d/%d]"
	warns := []RuleWarn{}
	tableMap := map[string]*schema.Table{}
	for _, t := range s.Tables {
		tableMap[t.Name] = t
	}
	for _, r := range s.Relations {
		delete(tableMap, r.Table.Name)
		delete(tableMap, r.ParentTable.Name)
	}
	if len(tableMap) > l.Max {
		warns = append(warns, RuleWarn{
			Message: fmt.Sprintf(msgFmt, len(tableMap), l.Max),
		})
	}
	return warns
}

// ColumnCount check table column count
type ColumnCount struct {
	Enabled bool `yaml:"enabled"`
	Max     int  `yaml:"max"`
}

// IsEnabled return Rule is enabled or not
func (l ColumnCount) IsEnabled() bool {
	return l.Enabled
}

// Check table column count
func (l ColumnCount) Check(s *schema.Schema) []RuleWarn {
	msgFmt := "%s has too many columns. [%d/%d]"
	warns := []RuleWarn{}
	for _, t := range s.Tables {
		if len(t.Columns) > l.Max {
			warns = append(warns, RuleWarn{
				Message: fmt.Sprintf(msgFmt, t.Name, len(t.Columns), l.Max),
			})
		}
	}
	return warns
}
