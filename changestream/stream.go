package changestream

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/SimonRichardson/nu-juju-watchers/db"
	"github.com/SimonRichardson/nu-juju-watchers/eventqueue"
	"gopkg.in/tomb.v2"
)

const (
	ChangePollInterval = time.Millisecond * 100
)

type change struct {
	id         int
	changeType eventqueue.ChangeType
	entityType string
	entityID   int64
	createdAt  string
}

func (c change) Type() eventqueue.ChangeType {
	return c.changeType
}

func (c change) EntityType() string {
	return c.entityType
}

func (c change) EntityID() int64 {
	return c.entityID
}

type ChangeStream struct {
	tomb     tomb.Tomb
	db       *sql.DB
	changeCh chan eventqueue.Change
	lastId   int
}

func New(db *sql.DB) *ChangeStream {
	stream := &ChangeStream{
		db:       db,
		changeCh: make(chan eventqueue.Change),
	}

	stream.tomb.Go(stream.loop)
	return stream
}

func (w *ChangeStream) Changes() <-chan eventqueue.Change {
	return w.changeCh
}

func (w *ChangeStream) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *ChangeStream) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}

func (w *ChangeStream) loop() error {
	defer close(w.changeCh)

	// Wait for 100 milliseconds for a change
	timer := time.NewTimer(ChangePollInterval)
	defer timer.Stop()

	for {
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying
		case <-timer.C:
			if err := db.WithRetry(w.read); err != nil {
				fmt.Println("ChangeStream err", err)
				return err
			}
			// TODO: We should make this adaptive.
			timer.Reset(ChangePollInterval)
		}
	}
}

const (
	query = `
SELECT MAX(id), type, entity_type, entity_id, MAX(created_at)
	FROM change_log WHERE id > ?
	GROUP BY type, entity_type, entity_id 
	ORDER BY id ASC
`
)

func (w *ChangeStream) read() error {
	// We want to last known Id we've scanned and everything after we've started
	// to subscribe.
	rows, err := w.db.Query(query, w.lastId)
	if err != nil {
		return err
	}
	defer rows.Close()

	var docs []change
	dest := func(i int) []interface{} {
		docs = append(docs, change{})
		return []interface{}{
			&docs[i].id,
			&docs[i].changeType,
			&docs[i].entityType,
			&docs[i].entityID,
			&docs[i].createdAt,
		}
	}
	for i := 0; rows.Next(); i++ {
		if err := rows.Scan(dest(i)...); err != nil {
			return err
		}
	}

	for _, chDoc := range docs {
		select {
		case w.changeCh <- chDoc:
			// done
		case <-w.tomb.Dying():
			return tomb.ErrDying
		}

		// Keep track of the last seen ID and MAX seen timestamp for the next poll.
		w.lastId = chDoc.id
	}

	return nil
}
