package spanner

import (
	"fmt"
	"strings"
)

// Database is Spanner Databaseを表す
type Database struct {
	ProjectID string
	Instance  string
	Database  string
}

// ToSpannerDatabaseName is Spanner Database Name として指定できる形式の文字列を返す
func (d *Database) ToSpannerDatabaseName() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", d.ProjectID, d.Instance, d.Database)
}

// SplitDatabaseName is projects/{PROJECT_ID}/instances/{INSTANCE}/databases/{DB} 形式の文字列をstructにして返す
func SplitDatabaseName(database string) (*Database, error) {
	l := strings.Split(database, "/")
	if len(l) < 6 {
		return nil, fmt.Errorf("invalid argument. The expected format is projects/{PROJECT_ID}/instances/{INSTANCE}/databases/{DB}. but get %s", database)
	}

	return &Database{
		ProjectID: l[1],
		Instance:  l[3],
		Database:  l[5],
	}, nil
}
