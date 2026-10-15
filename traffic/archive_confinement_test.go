package traffic

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Pause on the real recorder mutex after the exporter has created its output
// directory, then replace its ancestor. A path-only check before ExportHosts
// cannot protect writes following this deterministic swap.
func TestTrafficArchiveExportAncestorSwap(t *testing.T) {
	tr, _ := openTraffic(t)
	parent := filepath.Join(t.TempDir(), "stage")
	output := filepath.Join(parent, "payload", "traffic")
	outside := t.TempDir()
	victim := filepath.Join(outside, "payload", "traffic", "traffic.json")
	if err := os.MkdirAll(filepath.Dir(victim), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(victim, []byte("outside unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	tr.wmu.Lock()
	locked := true
	defer func() {
		if locked {
			tr.wmu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { _, err := tr.ExportHosts([]string{"swap-fixture.example.test"}, output); done <- err }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(output, "blobs")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("exporter did not reach its recorder-lock barrier")
		}
		time.Sleep(time.Millisecond)
	}
	if err := os.Rename(parent, parent+"-pinned"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, parent); err != nil {
		t.Fatal(err)
	}
	tr.wmu.Unlock()
	locked = false
	select {
	case err := <-done:
		t.Logf("confined export result after ancestor swap: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("export did not finish after releasing the recorder lock")
	}
	got, err := os.ReadFile(victim)
	if err != nil || string(got) != "outside unchanged" {
		t.Errorf("export escaped via swapped ancestor: got=%q err=%v", got, err)
	}
}
