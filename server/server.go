package server

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
)

type Server struct {
	db *sql.DB
}

func New(db *sql.DB) *Server {
	return &Server{db: db}
}

func (s Server) Serve(address string) (net.Listener, error) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		txn, err := s.db.BeginTx(r.Context(), nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		key := strings.TrimLeft(r.URL.Path, "/")
		result := ""
		switch r.Method {
		case "GET":
			row := txn.QueryRowContext(r.Context(), query, key)
			var (
				id    int
				key   string
				value string
			)
			if err = row.Scan(&id, &key, &value); err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
				if err := txn.Rollback(); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				break
			}
			if err := txn.Commit(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				break
			}
			result = fmt.Sprintf("get {%q:%q}", key, value)

		case "POST", "PUT":
			value, _ := ioutil.ReadAll(r.Body)
			result = fmt.Sprintf("upserted {%q:%q}", key, string(value))
			if _, err = txn.ExecContext(r.Context(), update, key, value); err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
				if err := txn.Commit(); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					break
				}
			}
			if err := txn.Commit(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				break
			}

		case "DELETE":
			result = fmt.Sprintf("deleted %q", key)
			if _, err = txn.ExecContext(r.Context(), remove, key); err != nil {
				result = fmt.Sprintf("Error: %s", err.Error())
				if err := txn.Commit(); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					break
				}
			}
			if err := txn.Commit(); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				break
			}

		default:
			result = fmt.Sprintf("Error: unsupported method %q", r.Method)

		}
		fmt.Fprintf(w, "%s\n", result)
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
