package main

import (
	"database/sql"
	"time"

	"gopkg.in/tomb.v2"
)

type WALType string

const (
	Create WALType = "c"
	Update WALType = "u"
	Delete WALType = "d"
)

type changeKey struct {
	walType     WALType
	contextName string
}

type Change struct {
	id          int
	walType     WALType
	contextName string
	contextID   int
	createdAt   time.Time
}

type ChangeStream struct {
	tomb tomb.Tomb
	db   *sql.DB
}

func (w *ChangeStream) Watch(out func(Change) error, stop <-chan struct{}) {
	w.tomb.Go(func() error {
		// Wait for 100 milliseconds for a change
		ticker := time.NewTicker(time.Millisecond * 100)

		now := time.Now()

		var lastId int
		for {
			select {
			case <-w.tomb.Dying():
				return tomb.ErrDying
			case <-stop:
				return nil
			case <-ticker.C:
				var err error
				if lastId, err = w.read(lastId, now, out); err != nil {
					return err
				}
			}
		}
	})
}

func (w *ChangeStream) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *ChangeStream) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}

func (w *ChangeStream) read(lastId int, now time.Time, out func(Change) error) (int, error) {
	txn, err := w.db.Begin()
	if err != nil {
		return -1, err
	}

	// We want to last known Id we've scanned and everything after we've started
	// to subscribe.
	rows, err := txn.Query(watch, lastId, now)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	var docs []Change
	dest := func(i int) []interface{} {
		docs = append(docs, Change{})
		return []interface{}{
			&docs[i].id,
			&docs[i].walType,
			&docs[i].contextName,
			&docs[i].contextID,
			&docs[i].createdAt,
		}
	}
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(dest(i)...); err != nil {
			return -1, txn.Rollback()
		}
	}

	if err := txn.Commit(); err != nil {
		return -1, err
	}

	// This only adds changes from the wal with the highest id, whilst
	// still retaining the order.
	witness := make(map[changeKey]struct{})
	updates := make([]Change, 0)
	for i := len(docs) - 1; i >= 0; i-- {
		doc := docs[i]
		ns := changeKey{
			walType:     doc.walType,
			contextName: doc.contextName,
		}
		if _, ok := witness[ns]; ok {
			continue
		}
		updates = append(updates, doc)
		witness[ns] = struct{}{}
	}
	for i := len(updates) - 1; i >= 0; i-- {
		if err := out(updates[i]); err != nil {
			return -1, err
		}
		lastId = updates[i].id
	}

	return lastId, nil
}
