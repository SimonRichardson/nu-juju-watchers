package main

import (
	"database/sql"
	"strings"

	"github.com/juju/collections/set"
	"github.com/pkg/errors"
	"gopkg.in/tomb.v2"
)

type ModelConfigValue struct {
	ID    int
	Key   string
	Value string
}

type ModelConfigWatcher struct {
	tomb tomb.Tomb
	db   *sql.DB
	out  chan []ModelConfigValue
	in   chan []Change
}

func NewModelConfigWatcher(db *sql.DB) *ModelConfigWatcher {
	return &ModelConfigWatcher{
		db:  db,
		out: make(chan []ModelConfigValue),
	}
}

func (w *ModelConfigWatcher) Run(in chan []Change) {
	w.tomb.Go(func() error {
		// Get the initial config.
		changes, err := w.initial()
		if err != nil {
			return err
		}

		out := w.out
		for {
			select {
			case <-w.tomb.Dying():
				return tomb.ErrDying

			case c := <-w.in:
				changes, err = w.updates(c)
				if err != nil {
					return err
				}
				if len(changes) > 0 {
					out = w.out
				}

			case out <- changes:
				out = nil
				changes = nil
			}
		}

	})
}

func (w *ModelConfigWatcher) Changes() <-chan []ModelConfigValue {
	return w.out
}

func (w *ModelConfigWatcher) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *ModelConfigWatcher) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}

func (w *ModelConfigWatcher) initial() ([]ModelConfigValue, error) {
	txn, err := w.db.Begin()
	if err != nil {
		return nil, err
	}

	rows, err := txn.Query(queryAll)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []ModelConfigValue
	dest := func(i int) []interface{} {
		docs = append(docs, ModelConfigValue{})
		return []interface{}{
			&docs[i].ID,
			&docs[i].Key,
			&docs[i].Value,
		}
	}
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(dest(i)...); err != nil {
			return nil, txn.Rollback()
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return docs, nil
}

func (w *ModelConfigWatcher) updates(changes []Change) ([]ModelConfigValue, error) {
	txn, err := w.db.Begin()
	if err != nil {
		return nil, err
	}

	idents := set.NewInts()
	for _, change := range changes {
		if change.walType == Delete {
			continue
		}
		idents.Add(change.id)
	}
	ints := idents.SortedValues()
	values := make([]interface{}, len(ints))
	args := make([]string, len(ints))
	for i, v := range ints {
		values[i] = v
		args[i] = "?"
	}

	query := strings.Replace(queryMultiple, "%?", strings.Join(args, ","), -1)
	rows, err := txn.Query(query, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []ModelConfigValue
	dest := func(i int) []interface{} {
		docs = append(docs, ModelConfigValue{})
		return []interface{}{
			&docs[i].ID,
			&docs[i].Key,
			&docs[i].Value,
		}
	}
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(dest(i)...); err != nil {
			return nil, txn.Rollback()
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	// Validate that we've got all the idents.
	if len(docs) != idents.Size() {
		return nil, errors.Errorf("missmatch of config values %d vs %d", idents.Size(), len(docs))
	}
	for _, doc := range docs {
		if !idents.Contains(doc.ID) {
			return nil, errors.Errorf("missing config id %d for key %s", doc.ID, doc.Key)
		}
	}

	return docs, nil
}
