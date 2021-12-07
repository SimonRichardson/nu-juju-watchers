package diff

import (
	"database/sql"

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
	tomb tomb.Tomb

	eventQueue EventQueue
	differ     differ
	out        chan []ModelConfigValue
}

func NewModelConfigWatcher(db *sql.DB, eventQueue EventQueue) *ModelConfigWatcher {
	watcher := &ModelConfigWatcher{
		out:        make(chan []ModelConfigValue),
		eventQueue: eventQueue,
	}

	watcher.differ = differ{
		pkFieldName: "id",
		findOneFn:   makeFindOneFn(db, query),
		findAllFn:   makeFindAllFn(db, queryAll),
		tomb:        watcher.tomb,
		out:         make(chan entityMap),
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

	// Start differ
	w.differ.subscription = subscription
	w.tomb.Go(w.differ.loop)

	for {
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying

		case entMap, ok := <-w.differ.out:
			if !ok {
				return nil
			}

			// Push change(s)
			changes := []ModelConfigValue{mapModelConfig(entMap)}
			select {
			case <-w.tomb.Dying():
				return tomb.ErrDying
			case w.out <- changes:
			}
		}
	}
}

func mapModelConfig(data entityMap) ModelConfigValue {
	return ModelConfigValue{
		ID:    data["id"].(int64),
		Key:   data["key"].(string),
		Value: data["value"].(string),
	}
}

const (
	query    = "SELECT id, key, value FROM model_config WHERE id = ?"
	queryAll = "SELECT id, key, value FROM model_config"
)
