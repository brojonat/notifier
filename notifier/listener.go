package notifier

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Listener interface connects to the database and allows callers to listen to a
// particular topic by issuing a LISTEN command. WaitForNotification blocks
// until receiving a notification or until the supplied context expires. The
// default implementation is tightly coupled to pgx (following River's
// implementation), but callers may implement their own listeners for any
// backend they'd like.
type Listener interface {
	Close(ctx context.Context) error
	Connect(ctx context.Context) error
	Listen(ctx context.Context, topic string) error
	Ping(ctx context.Context) error
	Unlisten(ctx context.Context, topic string) error
	WaitForNotification(ctx context.Context) (*Notification, error)
}

// NewListener return a Listener that draws a connection from the supplied Pool. This
// is somewhat discouraged
func NewListener(dbp *pgxpool.Pool) Listener {
	return &listener{
		mu:     sync.Mutex{},
		dbPool: dbp,
	}
}

type listener struct {
	conn   *pgxpool.Conn
	dbPool *pgxpool.Pool
	mu     sync.Mutex
}

// Close the connection to the database.
func (l *listener) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn == nil {
		return nil
	}

	// Release below would take care of cleanup and potentially put the
	// connection back into rotation, but in case a Listen was invoked without a
	// subsequent Unlisten on the same topic, close the connection explicitly to
	// guarantee no other caller will receive a partially tainted connection.
	err := l.conn.Conn().Close(ctx)

	// Even in the event of an error, make sure conn is set back to nil so that
	// the listener can be reused.
	l.conn.Release()
	l.conn = nil

	return err
}

// Connect to the database.
func (l *listener) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn != nil {
		return errors.New("connection already established")
	}

	conn, err := l.dbPool.Acquire(ctx)
	if err != nil {
		return err
	}

	l.conn = conn
	return nil
}

// Listen issues a LISTEN command for the supplied topic.
func (l *listener) Listen(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "LISTEN \""+topic+"\"")
	return err
}

// Ping the database
func (l *listener) Ping(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.conn.Ping(ctx)
}

// Unlisten issues an UNLISTEN from the supplied topic.
func (l *listener) Unlisten(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "UNLISTEN \""+topic+"\"")
	return err
}

// WaitForNotification blocks until receiving a notification and returns it. The
// pgx driver should maintain a buffer of notifications, so as long as Listen
// has been called, repeatedly calling WaitForNotification should yield all
// notifications.
func (l *listener) WaitForNotification(ctx context.Context) (*Notification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	pgn, err := l.conn.Conn().WaitForNotification(ctx)

	if err != nil {
		return nil, err
	}

	n := Notification{
		Channel: pgn.Channel,
		Payload: []byte(pgn.Payload),
	}

	return &n, nil
}
