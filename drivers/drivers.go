package drivers

import (
	"github.com/Melsoft-Games/tbls/schema"
)

// Driver is the common interface for database drivers
type Driver interface {
	Analyze(*schema.Schema) error
	Info() (*schema.Driver, error)
}
