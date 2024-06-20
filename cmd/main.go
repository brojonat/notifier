package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/brojonat/notifier/notifier"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {

	ctx := context.Background()
	l := getDefaultLogger(slog.LevelInfo)

	var url string
	flag.StringVar(&url, "dbhost", "", "DB host (postgresql://{user}:{password}@{hostname}/{db}?sslmode=require)")

	var topic string
	flag.StringVar(&topic, "channel", "", "a string")

	flag.Parse()

	if url == "" || topic == "" {
		fmt.Fprintf(os.Stderr, "missing required flag")
		os.Exit(1)
		return
	}

	// get a connection pool
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error connection to DB: %v", err)
		os.Exit(1)
	}
	if err = pool.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error pinging DB: %v", err)
		os.Exit(1)
	}

	// setup the listener
	li := notifier.NewListener(pool)
	if err := li.Connect(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error setting up listener: %v", err)
		os.Exit(1)
	}

	// setup the notifier
	n := notifier.NewNotifier(l, li)
	go n.Run(ctx)

	// subscribe to the topic
	sub := n.Listen(topic)

	// indefinitely listen for updates
	go func() {
		<-sub.EstablishedC()
		for {
			fmt.Printf("Got notification: %s\n", <-sub.NotificationC())
		}
	}()

	select {}
}

func getDefaultLogger(lvl slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     lvl,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.SourceKey {
				source, _ := a.Value.Any().(*slog.Source)
				if source != nil {
					source.Function = ""
					source.File = filepath.Base(source.File)
				}
			}
			return a
		},
	}))
}
