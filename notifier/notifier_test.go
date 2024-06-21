package notifier

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
)

// NB: these tests assume you have a postgres server listening on localhost:5432
// with username postgres and password postgres. You can trivially set this up
// with Docker with the following:
//
// docker run --rm --name postgres -p 5432:5432 \
// -e POSTGRES_PASSWORD=postgres postgres

func testPool(url string) (*pgxpool.Pool, error) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		return nil, err
	}
	return pool, nil
}

func TestNotifier(t *testing.T) {
	is := is.New(t)
	l := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	pool, err := testPool("postgresql://postgres:postgres@localhost:5432")
	is.NoErr(err)

	li := NewListener(pool)
	err = li.Connect(ctx)
	is.NoErr(err)

	n := NewNotifier(l, li)
	wg.Add(1)
	go func() {
		n.Run(ctx)
		wg.Done()
	}()
	sub := n.Listen("foo")

	conn, err := pool.Acquire(ctx)
	wg.Add(1)
	go func() {
		<-sub.EstablishedC()
		conn.Exec(ctx, "select pg_notify('foo', '1')")
		conn.Exec(ctx, "select pg_notify('foo', '2')")
		conn.Exec(ctx, "select pg_notify('foo', '3')")
		conn.Exec(ctx, "select pg_notify('foo', '4')")
		conn.Exec(ctx, "select pg_notify('foo', '5')")
		wg.Done()
	}()
	is.NoErr(err)

	wg.Add(1)

	out := make(chan string)
	go func() {
		<-sub.EstablishedC()
		for i := 0; i < 5; i++ {
			msg := <-sub.NotificationC()
			out <- string(msg)
		}
		close(out)
		wg.Done()
	}()

	msgs := []string{}
	for r := range out {
		msgs = append(msgs, r)
	}
	is.Equal(msgs, []string{"1", "2", "3", "4", "5"})

	cancel()
	sub.Unlisten(ctx) // uses background ctx anyway
	li.Close(ctx)
	wg.Wait()
}
