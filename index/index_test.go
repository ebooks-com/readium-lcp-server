// Copyright 2020 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package index

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/readium/readium-lcp-server/config"
)

func TestCRUD(t *testing.T) {

	config.Config.LcpServer.Database = "sqlite3://:memory:"
	driver, cnxn := config.GetDatabase(config.Config.LcpServer.Database)
	db, err := sql.Open(driver, cnxn)
	if err != nil {
		t.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	// Open() prepares the GetFromLicense statement, which joins the license table; the
	// index package only creates the content table for sqlite, so create a minimal
	// license table here or Prepare fails with "no such table: license".
	_, err = db.Exec("CREATE TABLE license (id varchar(255) PRIMARY KEY NOT NULL, content_fk varchar(255) NOT NULL)")
	if err != nil {
		t.Fatal(err)
	}

	idx, err := Open(db)
	if err != nil {
		t.Fatal(err)
	}

	c := Content{ID: "test20",
		EncryptionKey: []byte("1234"),
		Location:      "test.epub",
		Length:        1000,
		Sha256:        "xxxx",
		Type:          "epub"}

	err = idx.Add(c)
	if err != nil {
		t.Fatal(err)
	}
	cbis, err := idx.Get(c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if c.ID != cbis.ID {
		t.Fatal("Failed to Get back the record")
	}

	c.Location = "test.epub"
	err = idx.Update(c)
	if err != nil {
		t.Fatal(err)
	}

	c2 := Content{ID: "test21",
		EncryptionKey: []byte("1234"),
		Location:      "test2.epub",
		Length:        2000,
		Sha256:        "xxxx",
		Type:          "epub"}

	err = idx.Add(c2)
	if err != nil {
		t.Fatal(err)
	}

	fn := idx.List()
	contents := make([]Content, 0)

	for it, err := fn(); err == nil; it, err = fn() {
		contents = append(contents, it)
	}
	if len(contents) != 2 {
		t.Fatal("Failed to List two rows")
	}

	// GetFromLicense resolves content through the license.content_fk join.
	_, err = db.Exec("INSERT INTO license (id, content_fk) VALUES (?, ?)", "lic-1", c.ID)
	if err != nil {
		t.Fatal(err)
	}
	cFromLicense, err := idx.GetFromLicense("lic-1")
	if err != nil {
		t.Fatal(err)
	}
	if cFromLicense.ID != c.ID {
		t.Fatal("GetFromLicense returned the wrong content")
	}

}
