// Copyright 2020 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package licensestatuses

import (
	"database/sql"
	"errors"
	"log"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/dbutils"
	"github.com/readium/readium-lcp-server/status"
)

// ErrNotFound is license status not found
var ErrNotFound = errors.New("license Status not found")

// LicenseStatuses is an interface
type LicenseStatuses interface {
	GetByID(id int) (*LicenseStatus, error)
	Add(ls LicenseStatus) error
	List(deviceLimit int64, limit int64, offset int64) func() (LicenseStatus, error)
	GetByLicenseID(id string) (*LicenseStatus, error)
	GetByLicenseIDs(ids []string) (map[string]*LicenseStatus, error)
	Update(ls LicenseStatus) error
	Count(from time.Time, to time.Time) (int, error)
	CountWithStatus(from time.Time, to time.Time, status string) (int, error)
}

type dbLicenseStatuses struct {
	db               *sql.DB
	dbGet            *sql.Stmt
	dbList           *sql.Stmt
	dbGetByLicenseID *sql.Stmt
}

// Get retrieves a license status by id
func (i dbLicenseStatuses) GetByID(id int) (*LicenseStatus, error) {
	var statusDB int64
	ls := LicenseStatus{}

	var potentialRightsEnd *time.Time
	var licenseUpdate *time.Time
	var statusUpdate *time.Time

	row := i.dbGet.QueryRow(id)
	err := row.Scan(&ls.ID, &statusDB, &licenseUpdate, &statusUpdate, &ls.DeviceCount, &potentialRightsEnd, &ls.LicenseRef, &ls.CurrentEndLicense)

	if err == nil {
		status.GetStatus(statusDB, &ls.Status)

		ls.Updated = new(Updated)

		if (potentialRightsEnd != nil) && (!(*potentialRightsEnd).IsZero()) {
			ls.PotentialRights = new(PotentialRights)
			ls.PotentialRights.End = potentialRightsEnd
		}

		ls.Updated.Status = statusUpdate
		ls.Updated.License = licenseUpdate
		// fix an issue with clients which test that the date of last update of the license
		// is after the date of creation of the X509 certificate.
		// Associated with a fix to the license server.
		if config.Config.LcpServer.CertDate != "" {
			certDate, err := time.Parse("2006-01-02", config.Config.LcpServer.CertDate)
			if err == nil {
				if ls.Updated.License == nil || ls.Updated.License.Before(certDate) {
					ls.Updated.License = &certDate
				}
			}
		}
	} else {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
	}

	return &ls, err
}

// Add adds license status to database
func (i dbLicenseStatuses) Add(ls LicenseStatus) error {

	statusDB, err := status.SetStatus(ls.Status)
	if err == nil {
		var end *time.Time
		end = nil
		if ls.PotentialRights != nil && ls.PotentialRights.End != nil && !(*ls.PotentialRights.End).IsZero() {
			end = ls.PotentialRights.End
		}
		_, err = i.db.Exec(dbutils.GetParamQuery(config.Config.LsdServer.Database, `INSERT INTO license_status 
		(status, license_updated, status_updated, device_count, potential_rights_end, license_ref,  rights_end)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`),
			statusDB, ls.Updated.License, ls.Updated.Status, ls.DeviceCount, end, ls.LicenseRef, ls.CurrentEndLicense)
	}

	return err
}

// List gets license statuses which have devices count more than devices limit
// input parameters: limit - how many license statuses need to get, offset - from what position need to start
func (i dbLicenseStatuses) List(deviceLimit int64, limit int64, offset int64) func() (LicenseStatus, error) {

	var rows *sql.Rows
	var err error
	driver, _ := config.GetDatabase(config.Config.LsdServer.Database)
	if driver == "mssql" {
		rows, err = i.dbList.Query(deviceLimit, offset, limit)
	} else {
		rows, err = i.dbList.Query(deviceLimit, limit, offset)
	}
	if err != nil {
		return func() (LicenseStatus, error) { return LicenseStatus{}, err }
	}

	return func() (LicenseStatus, error) {
		var statusDB int64
		var err error

		ls := LicenseStatus{}
		ls.Updated = new(Updated)
		if rows.Next() {
			err = rows.Scan(&ls.ID, &statusDB, &ls.Updated.License, &ls.Updated.Status, &ls.DeviceCount, &ls.LicenseRef)

			if err == nil {
				status.GetStatus(statusDB, &ls.Status)
			}
		} else {
			rows.Close()
			err = ErrNotFound
		}
		return ls, err
	}
}

// GetByLicenseID gets license status by license id (uuid)
func (i dbLicenseStatuses) GetByLicenseID(licenseID string) (*LicenseStatus, error) {
	var statusDB int64
	ls := LicenseStatus{}

	var potentialRightsEnd *time.Time
	var licenseUpdate *time.Time
	var statusUpdate *time.Time

	row := i.dbGetByLicenseID.QueryRow(licenseID)
	err := row.Scan(&ls.ID, &statusDB, &licenseUpdate, &statusUpdate, &ls.DeviceCount, &potentialRightsEnd, &ls.LicenseRef, &ls.CurrentEndLicense)

	if err == nil {
		status.GetStatus(statusDB, &ls.Status)

		ls.Updated = new(Updated)

		if (potentialRightsEnd != nil) && (!(*potentialRightsEnd).IsZero()) {
			ls.PotentialRights = new(PotentialRights)
			ls.PotentialRights.End = potentialRightsEnd
		}

		ls.Updated.Status = statusUpdate
		ls.Updated.License = licenseUpdate
		// fix an issue with clients which test that the date of last update of the license
		// is after the date of creation of the X509 certificate.
		// Associated with a fix to the license server.
		if config.Config.LcpServer.CertDate != "" {
			certDate, err := time.Parse("2006-01-02", config.Config.LcpServer.CertDate)
			if err == nil {
				if ls.Updated.License == nil || ls.Updated.License.Before(certDate) {
					ls.Updated.License = &certDate
				}
			}
		}
	} else {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
	}

	return &ls, err
}

type licenseStatusIDRow struct {
	ID string `mssql:"id"`
}

// GetByLicenseIDs retrieves multiple license statuses by their license IDs in a single query
func (i dbLicenseStatuses) GetByLicenseIDs(licenseIDs []string) (map[string]*LicenseStatus, error) {
	if len(licenseIDs) == 0 {
		return make(map[string]*LicenseStatus), nil
	}

	var query string
	var args []interface{}

	// Check if we're using SQL Server
	driver, _ := config.GetDatabase(config.Config.LsdServer.Database)
	isMssql := driver == "mssql"

	if isMssql {
		// Use Table Valued Parameter for SQL Server to avoid parameter limit
		query = `SELECT l.id, l.status, l.license_updated, l.status_updated, l.device_count, 
			l.potential_rights_end, l.license_ref, l.rights_end
			FROM license_status l
			INNER JOIN ? t ON l.license_ref = t.id`

		tvpRows := make([]licenseStatusIDRow, len(licenseIDs))
		for idx, id := range licenseIDs {
			tvpRows[idx] = licenseStatusIDRow{ID: id}
		}

		tvp := mssql.TVP{
			TypeName: "LicenseStatusIDType",
			Value:    tvpRows,
		}
		args = []interface{}{tvp}

	} else {
		// Build the query with placeholders for other databases
		query = `SELECT id, status, license_updated, status_updated, device_count, potential_rights_end, license_ref, rights_end
		FROM license_status WHERE license_ref IN (`

		// Create placeholders and args
		args = make([]interface{}, len(licenseIDs))
		for idx, id := range licenseIDs {
			if idx > 0 {
				query += ", "
			}
			query += "?"
			args[idx] = id
		}
		query += ")"

		// Convert placeholders for the specific database
		query = dbutils.GetParamQuery(config.Config.LsdServer.Database, query)
	}

	rows, err := i.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make(map[string]*LicenseStatus)
	for rows.Next() {
		var statusDB int64
		ls := LicenseStatus{}

		var potentialRightsEnd *time.Time
		var licenseUpdate *time.Time
		var statusUpdate *time.Time

		err := rows.Scan(&ls.ID, &statusDB, &licenseUpdate, &statusUpdate, &ls.DeviceCount, &potentialRightsEnd, &ls.LicenseRef, &ls.CurrentEndLicense)
		if err != nil {
			return nil, err
		}

		status.GetStatus(statusDB, &ls.Status)

		ls.Updated = new(Updated)

		if (potentialRightsEnd != nil) && (!(*potentialRightsEnd).IsZero()) {
			ls.PotentialRights = new(PotentialRights)
			ls.PotentialRights.End = potentialRightsEnd
		}

		ls.Updated.Status = statusUpdate
		ls.Updated.License = licenseUpdate

		// fix an issue with clients which test that the date of last update of the license
		// is after the date of creation of the X509 certificate.
		if config.Config.LcpServer.CertDate != "" {
			certDate, err := time.Parse("2006-01-02", config.Config.LcpServer.CertDate)
			if err == nil {
				if ls.Updated.License == nil || ls.Updated.License.Before(certDate) {
					ls.Updated.License = &certDate
				}
			}
		}

		results[ls.LicenseRef] = &ls
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// Update updates a license status
func (i dbLicenseStatuses) Update(ls LicenseStatus) error {

	statusInt, err := status.SetStatus(ls.Status)
	if err != nil {
		return err
	}

	var potentialRightsEnd *time.Time

	if ls.PotentialRights != nil && ls.PotentialRights.End != nil && !(*ls.PotentialRights.End).IsZero() {
		potentialRightsEnd = ls.PotentialRights.End
	}

	var result sql.Result
	result, err = i.db.Exec(dbutils.GetParamQuery(config.Config.LsdServer.Database, `UPDATE license_status SET status=?, license_updated=?, status_updated=?, 
	device_count=?,potential_rights_end=?, rights_end=?  WHERE id=?`),
		statusInt, ls.Updated.License, ls.Updated.Status, ls.DeviceCount, potentialRightsEnd, ls.CurrentEndLicense, ls.ID)

	if err == nil {
		if r, _ := result.RowsAffected(); r == 0 {
			return ErrNotFound
		}
	}
	return err
}

// Count counts the number of license statuses in a time period
func (i dbLicenseStatuses) Count(from time.Time, to time.Time) (int, error) {

	var count int
	var err error

	row := i.db.QueryRow(dbutils.GetParamQuery(config.Config.LsdServer.Database, `SELECT COUNT(*) FROM license_status WHERE license_updated BETWEEN ? AND ?`),
		from, to)
	err = row.Scan(&count)
	return count, err
}

// CountWithStatus counts the number of license statuses in a time period
func (i dbLicenseStatuses) CountWithStatus(from time.Time, to time.Time, statusTxt string) (int, error) {

	var count int
	var err error

	statusInt, err := status.SetStatus(statusTxt)
	if err != nil {
		return 0, err
	}

	row := i.db.QueryRow(dbutils.GetParamQuery(config.Config.LsdServer.Database, `SELECT COUNT(*) FROM license_status WHERE status = ? AND license_updated BETWEEN ? AND ?`),
		statusInt, from, to)
	err = row.Scan(&count)
	return count, err
}

// Open defines scripts for queries & create table license_status if it does not exist
func Open(db *sql.DB) (l LicenseStatuses, err error) {

	driver, _ := config.GetDatabase(config.Config.LsdServer.Database)

	// if sqlite, create the license table if it does not exist
	if driver == "sqlite3" {
		_, err = db.Exec(tableDef)
		if err != nil {
			log.Println("Error creating license_status table")
			return
		}
	}

	dbGet, err := db.Prepare(dbutils.GetParamQuery(config.Config.LsdServer.Database, "SELECT * FROM license_status WHERE id = ?"))
	if err != nil {
		return
	}

	var dbList *sql.Stmt
	if driver == "mssql" {
		dbList, err = db.Prepare(`SELECT id, status, license_updated, status_updated, device_count, license_ref FROM license_status WHERE device_count >= ?
		ORDER BY id DESC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`)
	} else {
		dbList, err = db.Prepare(dbutils.GetParamQuery(config.Config.LsdServer.Database, `SELECT id, status, license_updated, status_updated, device_count, license_ref FROM license_status WHERE device_count >= ?
		ORDER BY id DESC LIMIT ? OFFSET ?`))

	}
	if err != nil {
		return
	}

	dbGetByLicenseID, err := db.Prepare(dbutils.GetParamQuery(config.Config.LsdServer.Database, "SELECT * FROM license_status where license_ref = ?"))
	if err != nil {
		return
	}

	l = dbLicenseStatuses{db, dbGet, dbList, dbGetByLicenseID}
	return
}

const tableDef = "CREATE TABLE IF NOT EXISTS license_status (" +
	"id INTEGER PRIMARY KEY," +
	"status int(11) NOT NULL," +
	"license_updated datetime NOT NULL," +
	"status_updated datetime NOT NULL," +
	"device_count int(11) DEFAULT NULL," +
	"potential_rights_end datetime DEFAULT NULL," +
	"license_ref varchar(255) NOT NULL," +
	"rights_end datetime DEFAULT NULL  " +
	");" +
	"CREATE INDEX IF NOT EXISTS license_ref_index on license_status (license_ref);"
