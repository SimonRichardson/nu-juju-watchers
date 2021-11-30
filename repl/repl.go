// Copyright 2021 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package repl

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/juju/clock"
	"github.com/juju/collections/set"
	"github.com/juju/errors"
	"github.com/juju/utils/v2"
)

const readTimeout = 5 * time.Second

type DBGetter interface {
	GetExistingDB(string) (*sql.DB, error)
}

type replSession struct {
	id string
	db *sql.DB

	// The params for the current command and a writer for encoding the
	// command result.
	cmdParams string
	resWriter io.Writer
}

type replCmdDef struct {
	descr   string
	handler func(*replSession)
}

type SQLRepl struct {
	connListener net.Listener
	dbGetter     DBGetter
	clock        clock.Clock

	sessionCtx      context.Context
	sessionCancelFn func()
	sessionGroup    sync.WaitGroup

	commands map[string]replCmdDef
}

func New(pathToSocket string, dbGetter DBGetter, clock clock.Clock) (*SQLRepl, error) {
	l, err := net.Listen("unix", pathToSocket)
	if err != nil {
		return nil, errors.Annotate(err, "creating UNIX socket for REPL sessions")
	}

	ctx, cancelFn := context.WithCancel(context.TODO())

	r := &SQLRepl{
		connListener:    l,
		dbGetter:        dbGetter,
		clock:           clock,
		sessionCtx:      ctx,
		sessionCancelFn: cancelFn,
	}
	r.registerCommands()
	go r.acceptConnections()

	return r, nil
}

// Kill implements the Worker interface. It closes the REPL socket and notifies
// any open sessions that they need to gracefully terminate.
func (r *SQLRepl) Kill() {
	r.connListener.Close()
}

// Wait implements the Worker interface. It blocks until all open REPL sessions
// terminate.
func (r *SQLRepl) Wait() error {
	r.sessionGroup.Wait()
	return nil
}

func (r *SQLRepl) registerCommands() {
	r.commands = map[string]replCmdDef{
		".help": {
			descr:   "display list of supported commands",
			handler: r.handleHelpCmd,
		},
		".open": {
			descr:   "connect to a database (e.g. '.open foo')",
			handler: r.handleOpenCommand,
		},
	}
}

func (r *SQLRepl) acceptConnections() {
	defer r.sessionGroup.Done()
	for {
		conn, err := r.connListener.Accept()
		if err != nil {
			return
		}

		r.sessionGroup.Add(1)
		go r.serveSession(conn)
	}
}

func (r *SQLRepl) serveSession(conn net.Conn) {
	sessionID, _ := utils.NewUUID()
	db, _ := r.dbGetter.GetExistingDB("foo")
	session := &replSession{
		id:        sessionID.String(),
		resWriter: conn,
		db:        db,
	}

	defer func() {
		r.sessionGroup.Done()
	}()

	// Render welcome banner and prompt
	_ = r.renderWelcomeBanner(conn)
	_, _ = fmt.Fprintf(conn, "\n%s> ", session.id)

	var cmdBuf = make([]byte, 4096)
	for {
		select {
		case <-r.sessionCtx.Done():
			// Make a best effort attempt to notify client that we are shutting down
			_, _ = fmt.Fprintf(conn, "\n*** REPL system is shutting down; terminating session\n")
			return
		default:
		}

		// Read input
		conn.SetReadDeadline(r.clock.Now().Add(readTimeout))
		n, err := conn.Read(cmdBuf)
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				continue // no command available
			}

			return
		} else if n == 0 {
			continue // no command available
		}

		// Process command and emit response
		r.processCommand(session, string(cmdBuf[:n]))

		// Render prompt
		_, _ = fmt.Fprintf(conn, "\n%s> ", session.id)
	}
}

func (r *SQLRepl) processCommand(s *replSession, input string) {
	tokens := strings.Fields(strings.TrimSpace(input))
	if len(tokens) == 0 {
		return
	}

	// Is it a known command?
	if cmd, known := r.commands[tokens[0]]; known {
		s.cmdParams = strings.Join(tokens[1:], " ")
		cmd.handler(s)
		return
	}

	// Is it an SQL select or insert?
	if strings.EqualFold(tokens[0], "SELECT") {
		s.cmdParams = strings.Join(tokens, " ")
		r.handleSelect(s)
		return
	}
	if strings.EqualFold(tokens[0], "INSERT") {
		s.cmdParams = strings.Join(tokens, " ")
		r.handleInsert(s)
		return
	}

	_, _ = fmt.Fprintf(s.resWriter, "Unknown command %q; for a list of supported commands type '.help'\n", tokens[0])
}

func (r *SQLRepl) renderWelcomeBanner(w io.Writer) error {
	_, err := fmt.Fprintln(w, `
Welcome to the REPL for accessing dqlite databases for Juju models.

Before running any commands you must first connect to a database. To connect
to a database, type '.open' followed by the model UUID to connect to.

For a list of supported commands type '.help'`)

	return err
}

func (r *SQLRepl) handleHelpCmd(s *replSession) {
	cmdSet := set.NewStrings()
	for cmdName := range r.commands {
		cmdSet.Add(cmdName)
	}

	_, _ = fmt.Fprintf(s.resWriter, "The REPL supports the following commands:\n")

	for _, cmdName := range cmdSet.SortedValues() {
		_, _ = fmt.Fprintf(s.resWriter, "%s\t%s\n", cmdName, r.commands[cmdName].descr)
	}

	_, _ = fmt.Fprintf(s.resWriter, "\nIn addition, you can also type SQL SELECT statements\n")
}

func (r *SQLRepl) handleOpenCommand(s *replSession) {
	var err error

	s.db, err = r.dbGetter.GetExistingDB(s.cmdParams)
	if errors.IsNotFound(err) {
		_, _ = fmt.Fprintf(s.resWriter, "No such database exists\n")
		return
	} else if err != nil {
		_, _ = fmt.Fprintf(s.resWriter, "Unable to acquire DB handle; check the logs for more details\n")
		return
	}

	_, _ = fmt.Fprintf(s.resWriter, "You are now connected to DB %q\n", s.cmdParams)
}

func (r *SQLRepl) handleInsert(s *replSession) {
	if s.db == nil {
		_, _ = fmt.Fprintf(s.resWriter, "Not connected to a database; use '.open' followed by the model UUID to connect to\n")
		return
	}

	if strings.Contains(strings.ToUpper(s.cmdParams), "DROP TABLE") {
		_, _ = fmt.Fprintf(s.resWriter, "I see you are a friend of little Bobby Drop Tables; say hi to him from me!\n")
		return
	}

	// NOTE(achilleasa): passing unfiltered user input to SQL is a horrible
	// horrible hack that should NEVER EVER see the light of day. You have
	// been warned!
	res, err := s.db.ExecContext(r.sessionCtx, s.cmdParams)
	if err != nil {
		_, _ = fmt.Fprintf(s.resWriter, "Unable to execute query; check the logs for more details\n")
		return
	}

	lastInsertID, _ := res.LastInsertId()
	rowsAffected, _ := res.RowsAffected()
	_, _ = fmt.Fprintf(s.resWriter, "Affected Rows: %d; last insert ID: %v\n", rowsAffected, lastInsertID)
}

func (r *SQLRepl) handleSelect(s *replSession) {
	if s.db == nil {
		_, _ = fmt.Fprintf(s.resWriter, "Not connected to a database; use '.open' followed by the model UUID to connect to\n")
		return
	}

	if strings.Contains(strings.ToUpper(s.cmdParams), "DROP TABLE") {
		_, _ = fmt.Fprintf(s.resWriter, "I see you are a friend of little Bobby Drop Tables; say hi to him from me!\n")
		return
	}

	// NOTE(achilleasa): passing unfiltered user input to SQL is a horrible
	// horrible hack that should NEVER EVER see the light of day. You have
	// been warned!
	res, err := s.db.QueryContext(r.sessionCtx, s.cmdParams)
	if err != nil {
		_, _ = fmt.Fprintf(s.resWriter, "Unable to execute query; check the logs for more details\n")
		return
	}

	// Render header
	colMeta, err := res.Columns()
	if err != nil {
		_, _ = fmt.Fprintf(s.resWriter, "Unable to obtain column list for query; check the logs for more details\n")
		return
	}
	_, _ = fmt.Fprintf(s.resWriter, "%s\n", strings.Join(colMeta, "\t"))

	// Render Rows
	fieldList := make([]interface{}, len(colMeta))
	for i := 0; i < len(fieldList); i++ {
		var field interface{}
		fieldList[i] = &field
	}

	var rowCount int
	for res.Next() {
		rowCount++
		res.Scan(fieldList...)
		for i := 0; i < len(colMeta); i++ {
			var delim = '\t'
			if i == len(colMeta)-1 {
				delim = '\n'
			}

			field := *(fieldList[i].(*interface{}))
			_, _ = fmt.Fprintf(s.resWriter, "%v%c", field, delim)
		}
	}

	if err := res.Err(); err != nil {
		_, _ = fmt.Fprintf(s.resWriter, "Error while iterating query result set; check the logs for more details\n")
		return
	}

	_, _ = fmt.Fprintf(s.resWriter, "\nTotal rows: %d\n", rowCount)
}
