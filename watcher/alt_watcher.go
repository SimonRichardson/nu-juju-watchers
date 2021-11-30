package watcher

import (
	"database/sql"

	"github.com/SimonRichardson/nu-juju-watchers/eventqueue"
	"gopkg.in/tomb.v2"
)

// WAMAGA: watchers made great again!

type AltModelConfigWatcher struct {
	tomb tomb.Tomb

	eventQueue EventQueue
	differ     differ
	out        chan []ModelConfigValue
}

func NewAlt(db *sql.DB, eventQueue EventQueue) *AltModelConfigWatcher {
	watcher := &AltModelConfigWatcher{
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

func (w *AltModelConfigWatcher) Changes() <-chan []ModelConfigValue {
	return w.out
}

func (w *AltModelConfigWatcher) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *AltModelConfigWatcher) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}

func (w *AltModelConfigWatcher) loop() error {
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

func mapModelConfig(data map[string]interface{}) ModelConfigValue {
	return ModelConfigValue{
		ID:    data["id"].(int64),
		Key:   data["key"].(string),
		Value: data["value"].(string),
	}
}
