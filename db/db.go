package db

import (
	"database/sql"
	"math/rand"
	"strings"
	"time"

	"github.com/canonical/go-dqlite/driver"
	"github.com/juju/errors"
	"github.com/mattn/go-sqlite3"
)

const (
	DefaultRetries = 250
	DefaultDelay   = time.Millisecond * 10
)

func WithRetry(fn func() error) error {
	for attempt := 0; attempt < DefaultRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil // all done
		}

		// No point in re-trying or logging a no-row error.
		if errors.Cause(err) == sql.ErrNoRows {
			return sql.ErrNoRows
		}

		if !IsRetriableError(err) {
			return err
		}

		jitter := time.Duration(rand.Float64() + 0.5)
		time.Sleep(DefaultDelay * jitter)
	}

	return errors.Errorf("unable to complete request after %d retries", DefaultRetries)
}

func WithRetryWithResult(fn func() (interface{}, error)) (interface{}, error) {
	var (
		res interface{}
		err error
	)

	WithRetry(func() error {
		res, err = fn()
		return err
	})

	return res, err
}

// IsRetriableError returns true if the given error might be transient and the
// interaction can be safely retried.
func IsRetriableError(err error) bool {
	err = errors.Cause(err)
	if err == nil {
		return false
	}

	if err, ok := err.(driver.Error); ok && err.Code == driver.ErrBusy {
		return true
	}

	if err == sqlite3.ErrLocked || err == sqlite3.ErrBusy {
		return true
	}

	if strings.Contains(err.Error(), "database is locked") {
		return true
	}

	if strings.Contains(err.Error(), "cannot start a transaction within a transaction") {
		return true
	}

	if strings.Contains(err.Error(), "bad connection") {
		return true
	}

	if strings.Contains(err.Error(), "checkpoint in progress") {
		return true
	}

	return false
}
