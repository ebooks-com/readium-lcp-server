// Copyright 2020 Readium Foundation. All rights reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.

package license

import (
	"testing"

	"github.com/readium/readium-lcp-server/config"
	"github.com/readium/readium-lcp-server/index"
)

func TestLicense(t *testing.T) {
	config.Config.Profile = "1.0"

	l := License{}
	contentID := "1234-1234-1234-1234"
	Initialize(contentID, &l)
	if l.ID == "" {
		t.Error("Should have an id")
	}

	err := SetLicenseProfile(&l)
	if err != nil {
		t.Error(err.Error())
	}
}

func useDefaultLinks(t *testing.T, links map[string]string) {
	t.Helper()
	saved := DefaultLinks
	DefaultLinks = links
	t.Cleanup(func() { DefaultLinks = saved })
}

func linkHref(t *testing.T, l License, rel string) string {
	t.Helper()
	for _, link := range l.Links {
		if link.Rel == rel {
			return link.Href
		}
	}
	t.Fatalf("no %q link in %+v", rel, l.Links)
	return ""
}

func setLinks(t *testing.T, sha256 string) License {
	t.Helper()
	l := License{ID: "lic-1", ContentID: "415101-3-9c06.lcpdf"}
	c := index.Content{
		Location: "Atlantic_History.lcpdf",
		Length:   2326979,
		Sha256:   sha256,
		Type:     "application/pdf+lcp",
	}
	if err := SetLicenseLinks(&l, c); err != nil {
		t.Fatal(err)
	}
	return l
}

func TestSetLicenseLinks_PublicationSha256(t *testing.T) {
	useDefaultLinks(t, map[string]string{
		"publication": "https://storage.example/{publication_id}?sha256={publication_sha256}",
	})

	l := setLinks(t, "+0xG/k==")

	want := "https://storage.example/415101-3-9c06.lcpdf?sha256=%2B0xG%2Fk%3D%3D"
	if got := linkHref(t, l, "publication"); got != want {
		t.Errorf("publication href = %q, want %q", got, want)
	}
}

func TestSetLicenseLinks_TemplateWithoutSha256(t *testing.T) {
	useDefaultLinks(t, map[string]string{
		"publication": "https://storage.example/{publication_id}",
	})

	l := setLinks(t, "+0xG/k==")

	want := "https://storage.example/415101-3-9c06.lcpdf"
	if got := linkHref(t, l, "publication"); got != want {
		t.Errorf("publication href = %q, want %q", got, want)
	}
}

func TestSetLicenseLinks_EmptySha256(t *testing.T) {
	useDefaultLinks(t, map[string]string{
		"publication": "https://storage.example/{publication_id}?sha256={publication_sha256}",
	})

	l := setLinks(t, "")

	want := "https://storage.example/415101-3-9c06.lcpdf?sha256="
	if got := linkHref(t, l, "publication"); got != want {
		t.Errorf("publication href = %q, want %q", got, want)
	}
}

func TestSetLicenseLinks_StatusAndHintExpandLicenseID(t *testing.T) {
	useDefaultLinks(t, map[string]string{
		"publication": "https://storage.example/{publication_id}",
		"status":      "https://lsd.example/licenses/{license_id}/status",
		"hint":        "https://lsd.example/licenses/{license_id}/hint",
	})

	l := setLinks(t, "+0xG/k==")

	if got, want := linkHref(t, l, "status"), "https://lsd.example/licenses/lic-1/status"; got != want {
		t.Errorf("status href = %q, want %q", got, want)
	}
	if got, want := linkHref(t, l, "hint"), "https://lsd.example/licenses/lic-1/hint"; got != want {
		t.Errorf("hint href = %q, want %q", got, want)
	}
}
