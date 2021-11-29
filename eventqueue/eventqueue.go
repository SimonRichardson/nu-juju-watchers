package eventqueue

import (
	"sync"

	"github.com/juju/collections/set"
	"github.com/pkg/errors"
	"gopkg.in/tomb.v2"
)

type ChangeType int

const (
	Create ChangeType = 1 << iota
	Update
	Delete
)

func (c ChangeType) String() string {
	var result string
	if (c & Create) != 0 {
		result += "c"
	}
	if (c & Update) != 0 {
		result += "u"
	}
	if (c & Delete) != 0 {
		result += "d"
	}
	return result
}

type Change interface {
	Type() ChangeType
	EntityType() string
	EntityID() int64
}

type Subscription interface {
	Close() error
	Changes() <-chan Change
}

type ChangeStream interface {
	Changes() <-chan Change
}

type subscription struct {
	id       int
	changeCh chan Change
	topics   set.Strings

	unsubFn func() error
}

func (s *subscription) Close() error {
	return s.unsubFn()
}

func (s *subscription) Changes() <-chan Change {
	return s.changeCh
}

type eventFilter struct {
	subscriptionID int
	changeMask     ChangeType
	filterFn       func(Change) bool
}

type EventQueue struct {
	tomb         tomb.Tomb
	changeStream ChangeStream

	mu            sync.Mutex
	subscriptions map[int]*subscription
	subsByTopic   map[string][]eventFilter
}

func New(changeStream ChangeStream) *EventQueue {
	eventQueue := &EventQueue{
		changeStream:  changeStream,
		subscriptions: make(map[int]*subscription),
		subsByTopic:   make(map[string][]eventFilter),
	}

	eventQueue.tomb.Go(eventQueue.loop)

	return eventQueue
}

func (s *EventQueue) Subscribe(opts ...SubscriptionOption) (Subscription, error) {
	if len(opts) == 0 {
		return nil, errors.Errorf("no subscription options specified")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create a new subscription and assign a unique ID to it.
	subID := len(s.subscriptions)
	sub := &subscription{
		id:       subID,
		changeCh: make(chan Change),
		topics:   set.NewStrings(),
		unsubFn:  func() error { return s.unsubscribe(subID) },
	}
	s.subscriptions[sub.id] = sub

	// Register filters to route changes matching the subscription criteria to the newly crated subscription.
	for _, opt := range opts {
		s.subsByTopic[opt.entityType] = append(s.subsByTopic[opt.entityType], eventFilter{
			subscriptionID: sub.id,
			changeMask:     opt.changeMask,
			filterFn:       opt.filterFn,
		})
		sub.topics.Add(opt.entityType)
	}

	return sub, nil
}

func (s *EventQueue) unsubscribe(subscriptionID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, found := s.subscriptions[subscriptionID]
	if !found {
		return nil
	}

	for topic := range sub.topics {
		var updatedFilters []eventFilter
		for _, filter := range s.subsByTopic[topic] {
			if filter.subscriptionID == subscriptionID {
				continue
			}
			updatedFilters = append(updatedFilters, filter)
		}
		s.subsByTopic[topic] = updatedFilters
	}

	delete(s.subscriptions, subscriptionID)
	close(sub.changeCh)
	return nil
}

func (s *EventQueue) loop() error {
	defer func() {
		for _, sub := range s.subscriptions {
			close(sub.changeCh)
		}
		s.subscriptions = make(map[int]*subscription)
	}()

	for {
		var (
			ch Change
			ok bool
		)
		select {
		case <-s.tomb.Dying():
			return tomb.ErrDying
		case ch, ok = <-s.changeStream.Changes():
			if !ok {
				return nil // EOF
			}
			// Read next change
		}

		s.mu.Lock()
		for _, subOpt := range s.subsByTopic[ch.EntityType()] {
			if (ch.Type() & subOpt.changeMask) == 0 {
				continue
			}

			if subOpt.filterFn != nil && !subOpt.filterFn(ch) {
				continue
			}

			select {
			case <-s.tomb.Dying():
				s.mu.Unlock()
				return tomb.ErrDying
			case s.subscriptions[subOpt.subscriptionID].changeCh <- ch:
				// pushed change.
			}
		}
		s.mu.Unlock()
	}
}

func (w *EventQueue) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *EventQueue) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}
