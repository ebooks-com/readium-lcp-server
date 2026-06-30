package dbutils

import (
	"bytes"
	"fmt"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
)

func getPostgresQuery(query string) string {
	var buffer bytes.Buffer
	idx := 1
	for _, char := range query {
		if char == '?' {
			buffer.WriteString(fmt.Sprintf("$%d", idx))
			idx += 1
		} else {
			buffer.WriteRune(char)
		}
	}
	return buffer.String()
}

// GetParamQuery replaces parameter placeholders '?' in the SQL query to
// placeholders supported by the selected database driver.
func GetParamQuery(database, query string) string {
	if strings.HasPrefix(database, "postgres") {
		return getPostgresQuery(query)
	} else {
		return query
	}
}

// VarcharParam binds a string as SQL Server varchar (non-Unicode). go-mssqldb
// otherwise sends Go strings as nvarchar; compared against a varchar key column
// (e.g. license.id) that forces a column-side CONVERT and turns an index seek into a
// full scan. Only apply it to varchar columns: wrapping a param for an nvarchar column
// is pointless and can down-encode a Unicode value. It is a no-op for every other
// driver, so callers stay database-agnostic. `database` is the driver URI
// (config.Config.*.Database).
func VarcharParam(database, v string) interface{} {
	if strings.HasPrefix(database, "mssql") {
		return mssql.VarChar(v)
	}
	return v
}
