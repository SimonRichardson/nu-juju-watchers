package watcher

import (
	"database/sql"
	"fmt"

	dbretry "github.com/SimonRichardson/nu-juju-watchers/db"
	"github.com/SimonRichardson/nu-juju-watchers/eventqueue"
	"gopkg.in/tomb.v2"
)

type entityMap map[string]interface{}

type differ struct {
	entityStore map[int64]entityMap

	pkFieldName string
	findOneFn   func(int64) (entityMap, error)
	findAllFn   func() ([]entityMap, error)

	// A subscription provided by the embedding worker; its lifecycle is managed
	// by the embedder.
	subscription eventqueue.Subscription

	tomb tomb.Tomb
	// @Simon: we should just simplify and push single entities from all watchers
	// if the consumer wants parallelism they can do that manually (see provisioner task changes)
	out chan entityMap
}

func (w *differ) Changes() <-chan entityMap {
	return w.out
}

func (w *differ) Wait() <-chan struct{} {
	return w.tomb.Dead()
}

func (w *differ) Close() error {
	w.tomb.Kill(nil)
	return w.tomb.Wait()
}

func (w *differ) loop() error {
	fmt.Println("differ loop start")
	// Populate store with initial state.
	if err := w.initialize(); err != nil {
		return err
	}

	fmt.Printf("differ initial state size: %d\n", len(w.entityStore))

	// Push initial state out.
	for _, ent := range w.entityStore {
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying
		case w.out <- ent:
		}
	}

	for {
		select {
		case <-w.tomb.Dying():
			return tomb.ErrDying

		case c, ok := <-w.subscription.Changes():
			if !ok {
				return nil
			}

			updatedEnt, err := w.processChange(c)
			if err != nil {
				fmt.Println("err", err)
				return err
			}

			if updatedEnt == nil || c.Type() == eventqueue.Delete {
				continue // no change or delete (TBD)
			}

			// Push changed entity.
			select {
			case <-w.tomb.Dying():
				return tomb.ErrDying
			case w.out <- updatedEnt:
			}
		}
	}
}

func (w *differ) initialize() error {
	w.entityStore = make(map[int64]entityMap)

	entities, err := w.findAllFn()
	if err != nil {
		return err
	}

	for _, ent := range entities {
		w.entityStore[ent[w.pkFieldName].(int64)] = ent
	}

	return nil
}

func (w *differ) processChange(change eventqueue.Change) (entityMap, error) {
	entID := change.EntityID()

	if (change.Type() & eventqueue.Delete) != 0 {
		oldEnt := w.entityStore[entID]
		delete(w.entityStore, entID)
		return oldEnt, nil
	}

	oldEnt := w.entityStore[entID]
	newEnt, err := w.findOneFn(entID) // TODO: handle sql.ErrNoRows
	if err != nil {
		return nil, err
	}

	if len(oldEnt) != len(newEnt) {
		w.entityStore[entID] = newEnt
		return newEnt, nil
	}

	// Compare keys
	for sKey, sVal := range oldEnt {
		if newEnt[sKey] != sVal {
			w.entityStore[entID] = newEnt
			return newEnt, nil
		}
	}
	for sKey, sVal := range newEnt {
		if oldEnt[sKey] != sVal {
			w.entityStore[entID] = newEnt
			return newEnt, nil
		}
	}

	return nil, nil // no change detected
}

func makeFindOneFn(db *sql.DB, query string) func(int64) (entityMap, error) {
	return func(pk int64) (entityMap, error) {
		ent, err := dbretry.WithRetryWithResult(func() (interface{}, error) {
			rs, err := db.Query(query, pk)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil, nil
				}
				return nil, err
			}
			defer rs.Close()

			columns, err := rs.Columns()
			if err != nil {
				return nil, err
			}

			for rs.Next() {
				return scanModelConfigMap(columns, rs.Scan)
			}
			return nil, nil
		})

		if err != nil {
			return nil, err
		}

		if ent != nil {
			return ent.(entityMap), nil
		}
		return nil, nil
	}
}

func makeFindAllFn(db *sql.DB, query string) func() ([]entityMap, error) {
	return func() ([]entityMap, error) {
		ent, err := dbretry.WithRetryWithResult(func() (interface{}, error) {
			rs, err := db.Query(query)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil, nil
				}
				return nil, err
			}
			defer rs.Close()

			colMeta, err := rs.Columns()
			if err != nil {
				return nil, err
			}

			var entList []entityMap
			for rs.Next() {
				ent, err := scanModelConfigMap(colMeta, rs.Scan)
				if err != nil {
					return nil, err
				}
				entList = append(entList, ent)
			}

			return entList, rs.Err()
		})

		if err != nil {
			return nil, err
		}

		return ent.([]entityMap), nil
	}
}

func scanModelConfigMap(colNames []string, scanFn func(...interface{}) error) (entityMap, error) {
	fieldList := make([]interface{}, len(colNames))
	for i := 0; i < len(fieldList); i++ {
		var field interface{}
		fieldList[i] = &field
	}

	if err := scanFn(fieldList...); err != nil {
		return nil, err
	}

	ent := make(entityMap)
	for i, colName := range colNames {
		ent[colName] = *(fieldList[i].(*interface{}))
	}
	return ent, nil
}
