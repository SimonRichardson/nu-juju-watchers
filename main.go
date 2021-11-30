package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/SimonRichardson/nu-juju-watchers/changestream"
	"github.com/SimonRichardson/nu-juju-watchers/eventqueue"
	"github.com/SimonRichardson/nu-juju-watchers/repl"
	"github.com/SimonRichardson/nu-juju-watchers/server"
	"github.com/SimonRichardson/nu-juju-watchers/watcher"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/juju/clock"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	doItLive()
}

func doItLive() {
	var api string
	var db string
	var join *[]string
	var dir string
	var verbose bool

	cmd := &cobra.Command{
		Use:   "nu-juju-watcher",
		Short: "Demo to show the nu-juju-watcher",
		Long:  `This demo shows the nu-juju-watchers driven by a WAL table`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logFunc := func(l client.LogLevel, format string, a ...interface{}) {
				if !verbose {
					return
				}
				log.Printf(fmt.Sprintf("%s: %s: %s\n", api, l.String(), format), a...)
			}

			// Setup up the database.
			app, err := app.New(dir, app.WithAddress(db), app.WithCluster(*join), app.WithLogFunc(logFunc))
			if err != nil {
				return err
			}
			if err := app.Ready(context.Background()); err != nil {
				return err
			}
			db, err := app.Open(context.Background(), "demo")
			if err != nil {
				return err
			}
			if _, err := db.Exec(schema); err != nil {
				return err
			}

			replSock := filepath.Join(dir, "juju.sock")
			_ = os.Remove(replSock)
			_, err = repl.New(replSock, dbGetter{db: db}, clock.WallClock)
			if err != nil {
				return err
			}

			// Create the server for adding new items to the database
			server := server.New(db)
			listener, err := server.Serve(api)
			if err != nil {
				return err
			}

			// Create the write ahead log watcher. This will notify any changes
			// that have occurred in the log.
			stream := changestream.New(db)
			defer stream.Close()

			eventQueue := eventqueue.New(stream)
			defer eventQueue.Close()

			// The NewModelConfigWatcher will take those changes and emit the
			// model configs based on any changes.
			modelConfigWatcher := watcher.New(db, eventQueue)
			defer modelConfigWatcher.Close()

			done := make(chan struct{}, 1)
			go func() {
				for {
					select {
					case <-done:

					// This is a proxy for anything that's wanting to watch
					// for changes. For now, our proxy just emits changes to
					// stdout.
					case change := <-modelConfigWatcher.Changes():
						fmt.Printf("%s: Changes from watcher: %v\n", dir, change)
					}
				}
			}()

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, unix.SIGPWR)
			signal.Notify(ch, unix.SIGINT)
			signal.Notify(ch, unix.SIGQUIT)
			signal.Notify(ch, unix.SIGTERM)
			signal.Notify(ch, unix.SIGKILL)
			select {
			case <-ch:
			case <-stream.Wait():
			case <-modelConfigWatcher.Wait():
			}

			close(done)

			listener.Close()
			db.Close()

			app.Handover(context.Background())
			app.Close()

			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&api, "api", "a", "", "address used to expose the demo API")
	flags.StringVarP(&db, "db", "d", "", "address used for internal database replication")
	join = flags.StringSliceP("join", "j", nil, "database addresses of existing nodes")
	flags.StringVarP(&dir, "dir", "D", "/tmp/dqlite-demo", "data directory")
	flags.BoolVarP(&verbose, "verbose", "v", false, "verbose logging")

	cmd.MarkFlagRequired("api")
	cmd.MarkFlagRequired("db")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

type dbGetter struct {
	db *sql.DB
}

func (g dbGetter) GetExistingDB(_ string) (*sql.DB, error) {
	return g.db, nil
}
