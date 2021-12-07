package watcher

import (
	"database/sql"

	"github.com/SimonRichardson/nu-juju-watchers/db"
	"github.com/SimonRichardson/nu-juju-watchers/eventqueue"
	"gopkg.in/tomb.v2"
)

const (
	stringsQuery    = "SELECT key FROM model_config WHERE id = ?"
	stringsQueryAll = "SELECT key FROM model_config"
)

func NewModelConfigKeyWatcher(db *sql.DB, eventQueue EventQueue) *StringsWatcher {
	watcher := &StringsWatcher{
		db:         db,
		eventQueue: eventQueue,
		out:        make(chan []string),

		tableName: "model_config",
		query:     stringsQuery,
		queryAll:  stringsQueryAll,
	}
	watcher.tomb.Go(watcher.loop)

	return watcher
}

type StringsWatcher struct {
	tomb       tomb.Tomb
	db         *sql.DB
	eventQueue EventQueue
	out        chan []string

	tableName string
	query     string
	queryAll  string
}

func (w *StringsWatcher) Changes() <-chan []string {
	return w.out
}

func (w *StringsWatcher) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *StringsWatcher) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}

func (w *StringsWatcher) loop() error {
	subscription, err := w.eventQueue.Subscribe(eventqueue.Topic(w.tableName, eventqueue.Create|eventqueue.Update))
	if err != nil {
		return err
	}
	defer subscription.Close()

	// Get the initial config.
	result, err := db.WithRetryWithResult(func() (interface{}, error) { return w.initial() })
	if err != nil {
		return err
	}
	changes := result.([]string)

	if len(changes) > 0 {
		// Push initial changes out.
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying
		case w.out <- changes:
		}
	}

	for {
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying

		case c, ok := <-subscription.Changes():
			if !ok {
				return nil
			}

			var changes []string
			err := db.WithRetry(func() error {
				var err error
				changes, err = w.updates(c)
				return err
			})
			if err != nil {
				return err
			}

			if len(changes) == 0 {
				continue
			}

			// Push new changes.
			select {
			case <-w.tomb.Dying():
				return tomb.ErrDying
			case w.out <- changes:
			}
		}
	}
}

func (w *StringsWatcher) initial() ([]string, error) {
	rows, err := w.db.Query(w.queryAll)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	var docs []string
	for i := 0; rows.Next(); i++ {
		docs = append(docs, "")
		if err := rows.Scan(&docs[i]); err != nil {
			return nil, err
		}
	}

	return docs, nil
}

func (w *StringsWatcher) updates(change eventqueue.Change) ([]string, error) {
	row := w.db.QueryRow(w.query, change.EntityID())

	var doc string
	err := row.Scan(&doc)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return []string{doc}, nil
}
