// Copyright 2020 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package index

import (
	"database/sql"
	"errors"
	"log"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/dbutils"
)

// ErrNotFound signals content not found
var ErrNotFound = errors.New("Content not found")

// Index is an interface
type Index interface {
	Get(id string) (Content, error)
	GetByIDs(ids []string) (map[string]Content, error)
	Add(c Content) error
	Update(c Content) error
	Delete(id string) error
	List() func() (Content, error)
}

// Content represents an encrypted resource
type Content struct {
	ID            string `json:"id"`
	EncryptionKey []byte `json:"key,omitempty"` // warning, sensitive info
	Location      string `json:"location"`
	Length        int64  `json:"length"`
	Sha256        string `json:"sha256"`
	Type          string `json:"type"`
}

type contentIDRow struct {
	ID string `mssql:"id"`
}

type dbIndex struct {
	db        *sql.DB
	dbGetByID *sql.Stmt
	dbList    *sql.Stmt
}

// Get returns a record by id
func (i dbIndex) Get(id string) (Content, error) {
	row := i.dbGetByID.QueryRow(id)
	var c Content
	err := row.Scan(&c.ID, &c.EncryptionKey, &c.Location, &c.Length, &c.Sha256, &c.Type)
	if err != nil {
		err = ErrNotFound
	}
	return c, err
}

// GetByIDs returns a set of records by ids
func (i dbIndex) GetByIDs(ids []string) (map[string]Content, error) {
	contents := make(map[string]Content)
	if len(ids) == 0 {
		return contents, nil
	}

	driver, _ := config.GetDatabase(config.Config.LcpServer.Database)
	var rows *sql.Rows
	var err error

	if driver == "mssql" {
		contentIDRows := make([]contentIDRow, len(ids))
		for idx, id := range ids {
			contentIDRows[idx] = contentIDRow{ID: id}
		}
		tvp := mssql.TVP{
			TypeName: "LicenseIDType",
			Value:    contentIDRows,
		}
		rows, err = i.db.Query("SELECT c.id, c.encryption_key, c.location, c.length, c.sha256, c.type FROM content c INNER JOIN ? t ON c.id = t.id", tvp)
	} else {
		query := "SELECT id, encryption_key, location, length, sha256, type FROM content WHERE id IN ("
		args := make([]interface{}, len(ids))
		placeholders := make([]string, len(ids))
		for idx, id := range ids {
			args[idx] = id
			placeholders[idx] = "?"
		}
		query += strings.Join(placeholders, ",") + ")"

		query = dbutils.GetParamQuery(config.Config.LcpServer.Database, query)
		rows, err = i.db.Query(query, args...)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var c Content
		if err := rows.Scan(&c.ID, &c.EncryptionKey, &c.Location, &c.Length, &c.Sha256, &c.Type); err != nil {
			return nil, err
		}
		contents[c.ID] = c
	}
	return contents, nil
}

// Add inserts a record
func (i dbIndex) Add(c Content) error {
	driver, _ := config.GetDatabase(config.Config.LcpServer.Database)

	if driver == "postgres" {
		_, err := i.db.Exec(dbutils.GetParamQuery(config.Config.LcpServer.Database, "INSERT INTO content (id,encryption_key,location,length,sha256,type) VALUES (?, ?::bytea, ?, ?, ?, ?)"),
			c.ID, c.EncryptionKey, c.Location, c.Length, c.Sha256, c.Type)
		return err

	} else {
		_, err := i.db.Exec("INSERT INTO content (id,encryption_key,location,length,sha256,type) VALUES (?, ?, ?, ?, ?, ?)",
			c.ID, c.EncryptionKey, c.Location, c.Length, c.Sha256, c.Type)
		return err
	}

}

// Update updates a record
func (i dbIndex) Update(c Content) error {
	driver, _ := config.GetDatabase(config.Config.LcpServer.Database)

	if driver == "postgres" {
		_, err := i.db.Exec(dbutils.GetParamQuery(config.Config.LcpServer.Database, "UPDATE content SET encryption_key=?::bytea , location=?, length=?, sha256=?, type=? WHERE id=?"),
			c.EncryptionKey, c.Location, c.Length, c.Sha256, c.Type, c.ID)
		return err
	} else {
		_, err := i.db.Exec("UPDATE content SET encryption_key=? , location=?, length=?, sha256=?, type=? WHERE id=?",
			c.EncryptionKey, c.Location, c.Length, c.Sha256, c.Type, c.ID)
		return err
	}

}

// Delete deletes a record
func (i dbIndex) Delete(id string) error {
	driver, _ := config.GetDatabase(config.Config.LcpServer.Database)

	if driver == "postgres" {
		_, err := i.db.Exec(dbutils.GetParamQuery(config.Config.LcpServer.Database, "DELETE FROM content WHERE id=?"), id)
		return err
	} else {
		_, err := i.db.Exec("DELETE FROM content WHERE id=?", id)
		return err
	}
}

// List lists rows
func (i dbIndex) List() func() (Content, error) {
	rows, err := i.dbList.Query()
	if err != nil {
		return func() (Content, error) { return Content{}, err }
	}
	return func() (Content, error) {
		var c Content
		var err error
		if rows.Next() {
			err = rows.Scan(&c.ID, &c.EncryptionKey, &c.Location, &c.Length, &c.Sha256, &c.Type)
		} else {
			rows.Close()
			err = ErrNotFound
		}
		return c, err
	}
}

// Open opens an SQL database and prepare db statements
func Open(db *sql.DB) (i Index, err error) {
	driver, _ := config.GetDatabase(config.Config.LcpServer.Database)

	// if sqlite, create the content table in the lcp db if it does not exist
	if driver == "sqlite3" {
		_, err = db.Exec(tableDef)
		if err != nil {
			log.Println("Error creating sqlite content table")
			return
		}
	}

	var dbGetByID *sql.Stmt
	dbGetByID, err = db.Prepare(dbutils.GetParamQuery(config.Config.LcpServer.Database, "SELECT id,encryption_key,location,length,sha256,type FROM content WHERE id = ?"))
	if err != nil {
		return
	}
	dbList, err := db.Prepare("SELECT id,encryption_key,location,length,sha256,type FROM content")
	if err != nil {
		return
	}
	i = dbIndex{db, dbGetByID, dbList}
	return
}

const tableDef = "CREATE TABLE IF NOT EXISTS content (" +
	"id varchar(255) PRIMARY KEY," +
	"encryption_key varchar(64) NOT NULL," +
	"location text NOT NULL," +
	"length bigint," +
	"sha256 varchar(64)," +
	"\"type\" varchar(255) NOT NULL default 'application/epub+zip')"
