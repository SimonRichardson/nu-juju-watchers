package server

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/SimonRichardson/nu-juju-watchers/db"
)

type Server struct {
	db *sql.DB
}

func New(db *sql.DB) *Server {
	return &Server{db: db}
}

func (s Server) Serve(address string) (net.Listener, error) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		key := strings.TrimLeft(r.URL.Path, "/")
		switch r.Method {
		case "GET":
			res, err := db.WithRetryWithResult(func() (interface{}, error) {
				return s.doRead(ctx, key)
			})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintln(w, err.Error())
				return
			}
			fmt.Fprintln(w, res)

		case "POST", "PUT":
			valueRaw, _ := ioutil.ReadAll(r.Body)
			value := strings.TrimSpace(string(valueRaw))

			res, err := db.WithRetryWithResult(func() (interface{}, error) {
				return s.doCreate(ctx, key, value)
			})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintln(w, err.Error())
				return
			}
			fmt.Fprintln(w, res)

		case "DELETE":
			res, err := db.WithRetryWithResult(func() (interface{}, error) {
				return s.doDelete(ctx, key)
			})
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintln(w, err.Error())
				return
			}
			fmt.Fprintln(w, res)

		default:
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Error: unsupported method %q\n", r.Method)
		}
	})

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	go http.Serve(listener, nil)

	return listener, err
}

const (
	query  = "SELECT id, key, value FROM model_config WHERE key = ?"
	update = "INSERT OR REPLACE INTO model_config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value"
	remove = "DELETE FROM model_config WHERE key = ?"
)

func (s *Server) doCreate(ctx context.Context, key, value string) (string, error) {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}

	if _, err = txn.ExecContext(ctx, update, key, value); err != nil {
		return "", err
	}

	if err := txn.Commit(); err != nil {
		return "", err
	}

	return fmt.Sprintf("upsert {%q:%q}", key, value), nil
}

func (s *Server) doRead(ctx context.Context, key string) (string, error) {
	row := s.db.QueryRowContext(ctx, query, key)
	config := struct {
		ID    int64
		Key   string
		Value string
	}{}

	if err := row.Scan(&config.ID, &config.Key, &config.Value); err != nil {
		if err == sql.ErrNoRows {
			return "(not found)", nil
		}
		return "", err
	}

	return fmt.Sprintf("get {%q:%q}", config.Key, config.Value), nil
}

func (s *Server) doDelete(ctx context.Context, key string) (string, error) {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}

	if _, err = txn.ExecContext(ctx, remove, key); err != nil {
		return "", err
	}

	if err := txn.Commit(); err != nil {
		return "", err
	}

	return fmt.Sprintf("removed {%q}", key), nil
}
