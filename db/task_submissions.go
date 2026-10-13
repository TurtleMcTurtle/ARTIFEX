package db

import "errors"

var ErrSubmissionConflict = errors.New("submission conflicts with existing request")
