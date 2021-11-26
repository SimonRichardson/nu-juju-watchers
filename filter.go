package main

import (
	"gopkg.in/tomb.v2"
)

type DocState int

const (
	CreateDoc DocState = 1
	UpdateDoc DocState = 2
	DeleteDoc DocState = 4
)

type Watcher interface {
	Watch(func(Change) error, <-chan struct{})
}

type ModelConfigFilter struct {
	tomb    tomb.Tomb
	watcher Watcher
}

func NewModelConfigFilter(watcher Watcher) *ModelConfigFilter {
	filter := &ModelConfigFilter{
		watcher: watcher,
	}

	return filter
}

func (s *ModelConfigFilter) Subscribe(table string, modification DocState) (chan []ModelConfigChange, func()) {
	in := make(chan Change)
	out := make(chan []ModelConfigChange)

	stop := make(chan struct{}, 1)

	s.watcher.Watch(func(change Change) error {
		// We don't care for anything that isn't the table we're looking for.
		if change.contextName != table {
			return nil
		}

		in <- change

		return nil
	}, stop)

	go func() {
		changes := make([]ModelConfigChange, 0)
		for {
			select {
			case value := <-in:
				var state DocState
				if value.walType == Create {
					state |= CreateDoc
				} else if value.walType == Update {
					state |= UpdateDoc
				} else if value.walType == Delete {
					state |= DeleteDoc
				}

				if (state & modification) == 0 {
					continue
				}

				changes = append(changes, ModelConfigChange{
					ID:    value.contextID,
					State: state,
				})
			default:
				out <- changes
				changes = changes[:0]
			}
		}

	}()

	return out, func() {
		close(stop)
	}
}

func (w *ModelConfigFilter) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *ModelConfigFilter) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}
