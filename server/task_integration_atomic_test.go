package server

import (
	"github.com/sebastian93921/artifex/db"
	"sync"
	"testing"
)

func TestIntegrationDatabaseConcurrentSubmission(t *testing.T) {
	s, _ := integrationFixture(t)
	opts := db.TaskCreateOptions{SubmissionOwner: "ARTIFEX", ExternalRequestID: "db-concurrent", RequestHash: "fixture-hash"}
	var wg sync.WaitGroup
	results := make(chan *db.Task, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := s.m.pg.CreateTaskWithOptions("inert", "inert", opts)
			if err != nil {
				errs <- err
			} else {
				results <- task
			}
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var id int64
	created := 0
	for task := range results {
		if id != 0 && id != task.ID {
			t.Fatal("duplicate task")
		}
		id = task.ID
		if !task.Replay {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created %d", created)
	}
	s.m.pg.Exec(`UPDATE tasks SET owner='other' WHERE id=$1`, id)
	if _, err := s.m.pg.CreateTaskWithOptions("inert", "inert", opts); err != db.ErrSubmissionConflict {
		t.Fatalf("foreign replay %v", err)
	}
}
