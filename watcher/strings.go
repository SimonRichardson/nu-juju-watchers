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
	subscription, err := w.eventQueue.Subscribe(eventqueue.Topic(w.tableName, eventqueue.Create|eventqueue.Update|eventqueue.Delete))
	if err != nil {
		return err
	}
	defer subscription.Close()

	changes, err := w.initial()
	if err != nil {
		return err
	}

	out := w.out
	for {
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying
		case c, ok := <-subscription.Changes():
			if !ok {
				return nil
			}

			changes, err := w.updates(c)
			if err != nil {
				return err
			}

			if len(changes) == 0 {
				continue
			}

			out = w.out
		case out <- changes:
		}
	}
}

func (w *StringsWatcher) initial() ([]string, error) {
	var docs []string
	err := db.WithRetry(func() error {
		rows, err := w.db.Query(w.queryAll)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		defer rows.Close()

		for i := 0; rows.Next(); i++ {
			docs = append(docs, "")
			if err := rows.Scan(&docs[i]); err != nil {
				return err
			}
		}

		return nil
	})

	return docs, err
}

func (w *StringsWatcher) updates(change eventqueue.Change) ([]string, error) {
	var doc string
	err := db.WithRetry(func() error {
		row := w.db.QueryRow(w.query, change.EntityID())

		err := row.Scan(&doc)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}

		return nil
	})
	return []string{doc}, err
}
