package watcher

import (
	"database/sql"

	"github.com/SimonRichardson/nu-juju-watchers/db"
	"github.com/SimonRichardson/nu-juju-watchers/eventqueue"
	"gopkg.in/tomb.v2"
)

type EventQueue interface {
	Subscribe(opts ...eventqueue.SubscriptionOption) (eventqueue.Subscription, error)
}

type ModelConfigValue struct {
	ID    int64
	Key   string
	Value string
}

type ModelConfigWatcher struct {
	tomb       tomb.Tomb
	db         *sql.DB
	eventQueue EventQueue
	out        chan []ModelConfigValue
}

func NewModelConfigWatcher(db *sql.DB, eventQueue EventQueue) *ModelConfigWatcher {
	watcher := &ModelConfigWatcher{
		db:         db,
		eventQueue: eventQueue,
		out:        make(chan []ModelConfigValue),
	}
	watcher.tomb.Go(watcher.loop)

	return watcher
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

func (w *ModelConfigWatcher) loop() error {
	subscription, err := w.eventQueue.Subscribe(eventqueue.Topic("model_config", eventqueue.Create|eventqueue.Update|eventqueue.Delete))
	if err != nil {
		return err
	}
	defer subscription.Close()

	// Get the initial config.
	result, err := db.WithRetryWithResult(func() (interface{}, error) { return w.initial() })
	if err != nil {
		return err
	}
	changes := result.([]ModelConfigValue)

	store := make(map[int64]ModelConfigValue)
	if len(changes) > 0 {
		for _, change := range changes {
			store[change.ID] = change
		}

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

			// Modifications can be create or update.
			var modifications []ModelConfigValue
			var deletions map[int64]struct{}
			err := db.WithRetry(func() error {
				var err error
				modifications, deletions, err = w.updates(c)
				return err
			})
			if err != nil {
				return err
			}

			changes := diffStoreChanges(store, modifications)

			// Store the changes.
			for _, v := range changes {
				store[v.ID] = v
			}
			for id := range deletions {
				delete(store, id)
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

func diffStoreChanges(store map[int64]ModelConfigValue, changes []ModelConfigValue) []ModelConfigValue {
	results := make([]ModelConfigValue, 0)
	for _, change := range changes {
		ch, ok := store[change.ID]
		if ok && ch.Value == change.Value {
			continue
		}

		results = append(results, change)

	}
	return results
}

const (
	modelConfigQuery    = "SELECT id, key, value FROM model_config WHERE id = ?"
	modelConfigQueryAll = "SELECT id, key, value FROM model_config"
)

func (w *ModelConfigWatcher) initial() ([]ModelConfigValue, error) {
	rows, err := w.db.Query(modelConfigQueryAll)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
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
			return nil, err
		}
	}

	return docs, nil
}

func (w *ModelConfigWatcher) updates(change eventqueue.Change) ([]ModelConfigValue, map[int64]struct{}, error) {
	if (change.Type() & eventqueue.Delete) != 0 {
		return nil, map[int64]struct{}{change.EntityID(): {}}, nil
	}

	row := w.db.QueryRow(modelConfigQuery, change.EntityID())

	var doc ModelConfigValue
	err := row.Scan(&doc.ID, &doc.Key, &doc.Value)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	return []ModelConfigValue{doc}, nil, nil
}
