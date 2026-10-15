package traffic

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlobGCFailsWhenLegacyReferencesCannotBeInspected(t *testing.T) {
	tr, dir := openTraffic(t)
	const hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	legacy := filepath.Join(dir, "legacy-gc.example.test", "request", "fixture")
	if err := os.MkdirAll(legacy, 0700); err != nil {
		t.Fatal(err)
	}
	request := filepath.Join(legacy, "request.http")
	if err := os.WriteFile(request, []byte("@blob sha256:"+hash), 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(request, 0600) })
	blob := filepath.Join(dir, "_blobs", "sha256", hash[:2], hash+".bin")
	if err := os.MkdirAll(filepath.Dir(blob), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, []byte("legacy referenced body"), 0600); err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(dir, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.DB().Exec(`INSERT INTO exchanges(id,host,path) VALUES('legacy-gc-fixture','legacy-gc.example.test',?)`, rel); err != nil {
		t.Fatal(err)
	}
	tr.wmu.Lock()
	err = tr.gcBlobs()
	tr.wmu.Unlock()
	if err == nil {
		t.Error("GC succeeded without inspecting an active legacy reference")
	}
	if got, err := os.ReadFile(blob); err != nil || string(got) != "legacy referenced body" {
		t.Errorf("GC removed a body whose active legacy reference was unreadable: got=%q err=%v", got, err)
	}
}

func TestArchiveBlobCleanupErrorsAreReturned(t *testing.T) {
	for _, failure := range []struct {
		name string
		mode os.FileMode
	}{{"unlink", 0500}, {"inspect", 0100}} {
		t.Run(failure.name, func(t *testing.T) {
			tr, dir := openTraffic(t)
			const host = "blob-error.example.test"
			body := []byte(strings.Repeat("B", maxInlineBody+1024))
			tr.record(newFlow(host, "GET", "/body", nil, body, withRespType("application/octet-stream")))
			var hash string
			if err := tr.DB().QueryRow(`SELECT resp_blob FROM exchange_bodies`).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "_blobs", "sha256", hash[:2], hash+".bin")
			bucket := filepath.Dir(path)
			if err := os.Chmod(bucket, failure.mode); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(bucket, 0700) })
			stage, err := tr.StageDeleteHostsExactForArchive([]string{host}, 77, 42)
			if err != nil {
				t.Fatal(err)
			}
			if err := stage.Commit(); err == nil || !strings.Contains(err.Error(), "Reclaim traffic blobs") {
				t.Errorf("archive Commit suppressed %s failure while blob remained: %v", failure.name, err)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != string(body) {
				t.Errorf("fixture did not leave its exclusive blob: size=%d err=%v", len(got), err)
			}
			var refs int
			if err := tr.DB().QueryRow(`SELECT count(*) FROM blob_refs WHERE hash=?`, hash).Scan(&refs); err != nil || refs != 0 {
				t.Errorf("failure fixture did not commit reference deletion: refs=%d err=%v", refs, err)
			}
		})
	}
}
