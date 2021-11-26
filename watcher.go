package main

import (
	"database/sql"
	"strings"

	"github.com/juju/collections/set"
	"github.com/juju/pubsub/v2"
	"github.com/pkg/errors"
	"gopkg.in/tomb.v2"
)

type ModelConfigValue struct {
	ID    int
	Key   string
	Value string
}

type ModelConfigChange struct {
	ID    int
	State DocState
}

type ModelConfigWatcher struct {
	tomb tomb.Tomb
	db   *sql.DB
	out  chan []ModelConfigValue
	in   chan []ModelConfigChange
}

func NewModelConfigWatcher(db *sql.DB, source chan []ModelConfigChange) *ModelConfigWatcher {
	return &ModelConfigWatcher{
		db:  db,
		out: make(chan []ModelConfigValue),
		in:  source,
	}
}

func (w *ModelConfigWatcher) Run() {
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

func (w *ModelConfigWatcher) updates(changes []ModelConfigChange) ([]ModelConfigValue, error) {
	txn, err := w.db.Begin()
	if err != nil {
		return nil, err
	}

	idents := set.NewInts()
	for _, change := range changes {
		if change.State == DeleteDoc {
			continue
		}
		idents.Add(change.ID)
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

type DocState int

const (
	CreateDoc DocState = iota
	UpdateDoc
	DeleteDoc
)

type ModelConfigSource struct {
	tomb  tomb.Tomb
	unsub func()
	in    chan ModelConfigChange
	out   chan []ModelConfigChange
}

func NewModelConfigSource(events *pubsub.SimpleHub) *ModelConfigSource {
	source := &ModelConfigSource{
		in:  make(chan ModelConfigChange),
		out: make(chan []ModelConfigChange),
	}
	source.tomb.Go(source.loop)

	source.unsub = events.Subscribe("wal_change", func(topic string, data interface{}) {
		c, ok := data.(Change)
		if !ok {
			panic("programming error, expected a Change")
		}

		// If it's something we're not interested in, skip.
		if c.contextName != "model_config" {
			return
		}

		state := UpdateDoc
		switch c.walType {
		case Create:
			state = CreateDoc
		case Delete:
			state = DeleteDoc
		}

		source.in <- ModelConfigChange{
			ID:    c.contextID,
			State: state,
		}
	})

	return source
}

func (s *ModelConfigSource) Changes() chan []ModelConfigChange {
	return s.out
}

func (w *ModelConfigSource) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *ModelConfigSource) Close() error {
	w.unsub()

	w.tomb.Kill(nil)
	return w.tomb.Wait()
}

func (s *ModelConfigSource) loop() error {
	for {
		select {
		case <-s.tomb.Dying():
			return tomb.ErrDying
		case in := <-s.in:
			// TODO: Batch inputs.
			s.out <- []ModelConfigChange{in}
		}
	}
}
