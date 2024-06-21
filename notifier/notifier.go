package notifier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// Notifier interface wraps a Listener. It holds a single Postgres connection
// per process, allows other components in the same program to use it to
// subscribe to any number of topics, waits for notifications, and distributes
// them to listening components as they’re received
type Notifier interface {
	// Returns a Subscription to the supplied channel topic which can be used to by
	// the caller to receive data published to that channel
	Listen(channel string) Subscription

	// this runs the receiving loop forever
	Run(ctx context.Context) error
}

// Subscription provides a means to listen on a particular topic. Notifiers
// return Subscriptions that callers can use to receive updates.
type Subscription interface {
	NotificationC() <-chan []byte
	EstablishedC() <-chan struct{}
	Unlisten(ctx context.Context)
}

// Notification encapsulates a published message
type Notification struct {
	Channel string `json:"channel"`
	Payload []byte `json:"payload"`
}

type subscription struct {
	channel    string
	listenChan chan []byte
	notifier   *notifier

	establishedChan      chan struct{}
	establishedChanClose func()
	unlistenOnce         sync.Once
}

// NotificationC returns the underlying notification channel.
func (s *subscription) NotificationC() <-chan []byte { return s.listenChan }

// EstablishedC is a channel that's closed after the Notifier has successfully
// established a connection to the database and started listening for updates.
//
// There's no full guarantee that the notifier will successfully establish a
// listen, so callers will usually want to `select` on it combined with a
// context done, a stop channel, and/or a timeout.
func (s *subscription) EstablishedC() <-chan struct{} { return s.establishedChan }

// Unlisten unregisters the subscriber from its notifier
func (s *subscription) Unlisten(ctx context.Context) {
	s.unlistenOnce.Do(func() {
		// Unlisten uses background context in case of cancellation.
		if err := s.notifier.unlisten(context.Background(), s); err != nil {
			s.notifier.logger.Error("error unlistening on channel", "err", err, "channel", s.channel)
		}
	})
}

type notifier struct {
	mu                        sync.RWMutex
	logger                    *slog.Logger
	listener                  Listener
	subscriptions             map[string][]*subscription
	channelChanges            []channelChange
	waitForNotificationCancel context.CancelFunc
}

func NewNotifier(l *slog.Logger, li Listener) Notifier {
	return &notifier{
		mu:                        sync.RWMutex{},
		logger:                    l,
		listener:                  li,
		subscriptions:             make(map[string][]*subscription),
		channelChanges:            []channelChange{},
		waitForNotificationCancel: context.CancelFunc(func() {}),
	}
}

type channelChange struct {
	channel   string
	close     func()
	operation string
}

// Listen returns a Subscription.
func (n *notifier) Listen(channel string) Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()

	existingSubs := n.subscriptions[channel]

	sub := &subscription{
		channel:    channel,
		listenChan: make(chan []byte, 2),
		notifier:   n,
	}
	n.subscriptions[channel] = append(existingSubs, sub)

	if len(existingSubs) > 0 {
		// If there's already another subscription for this channel, reuse its
		// established channel. It may already be closed (to indicate that the
		// connection is established), but that's okay.
		sub.establishedChan = existingSubs[0].establishedChan
		sub.establishedChanClose = func() {} // no op since not channel owner

		return sub
	}

	// The notifier will close this channel after it's successfully established
	// `LISTEN` for the given channel. Gives subscribers a way to confirm a
	// listen before moving on, which is especially useful in tests.
	sub.establishedChan = make(chan struct{})
	sub.establishedChanClose = sync.OnceFunc(func() { close(sub.establishedChan) })

	n.channelChanges = append(n.channelChanges,
		channelChange{channel, sub.establishedChanClose, "listen"})

	// Cancel out of blocking on WaitForNotification so changes can be processed
	// immediately.
	n.waitForNotificationCancel()

	return sub
}

const listenerTimeout = 10 * time.Second

// Listens on a topic with an appropriate logging statement. Should be preferred
// to `listener.Listen` for improved logging/telemetry.
func (n *notifier) listenerListen(ctx context.Context, channel string) error {
	ctx, cancel := context.WithTimeout(ctx, listenerTimeout)
	defer cancel()

	n.logger.Debug("listening on channel", "channel", channel)
	if err := n.listener.Listen(ctx, channel); err != nil {
		return fmt.Errorf("error listening on channel %q: %w", channel, err)
	}

	return nil
}

// Unlistens on a topic with an appropriate logging statement. Should be
// preferred to `listener.Unlisten` for improved logging/telemetry.
func (n *notifier) listenerUnlisten(ctx context.Context, channel string) error {
	ctx, cancel := context.WithTimeout(ctx, listenerTimeout)
	defer cancel()

	n.logger.Debug("unlistening on channel", "channel", channel)
	if err := n.listener.Unlisten(ctx, string(channel)); err != nil {
		return fmt.Errorf("error unlistening on channel %q: %w", channel, err)
	}

	return nil
}

// this needs to pull channelChange instances from the channelChange channel
// in order to perform LISTEN/UNLISTEN operations on the notifier.
func (n *notifier) processChannelChanges(ctx context.Context) error {
	n.logger.Debug("processing channel changes...")
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, u := range n.channelChanges {
		switch u.operation {
		case "listen":
			n.logger.Debug("listening to new channel", "channel", u.channel)
			n.listenerListen(ctx, u.channel)
			u.close()
		case "unlisten":
			n.logger.Debug("unlistening from channel", "channel", u.channel)
			n.listenerUnlisten(ctx, u.channel)
		default:
			n.logger.Error("got unexpected change operation", "operation", u.operation)
		}
	}
	return nil
}

// waitOnce blocks until either 1) a notification is received and
// distributed to all topic listeners, 2) the timeout is hit, or 3) an external
// caller calls l.waitForNotificationCancel. In all 3 cases, nil is returned to
// signal good/expected exit conditions, meaning a caller can simply call
// handleNextNotification again.
func (n *notifier) waitOnce(ctx context.Context) error {
	if err := n.processChannelChanges(ctx); err != nil {
		return err
	}

	// WaitForNotification is a blocking function, but since we want to wake
	// occasionally to process new `LISTEN`/`UNLISTEN`, we let the context
	// timeout and also expose a way for external code to cancel this loop with
	// waitForNotificationCancel.
	notification, err := func() (*Notification, error) {
		const listenTimeout = 30 * time.Second

		ctx, cancel := context.WithTimeout(ctx, listenTimeout)
		defer cancel()

		// Provides a way for the blocking wait to be cancelled in case a new
		// subscription change comes in.
		n.mu.Lock()
		n.waitForNotificationCancel = cancel
		n.mu.Unlock()

		notification, err := n.listener.WaitForNotification(ctx)
		if err != nil {
			return nil, fmt.Errorf("error waiting for notification: %w", err)
		}

		return notification, nil
	}()
	if err != nil {
		// If the error was a cancellation or the deadline being exceeded but
		// there's no error in the parent context, return no error.
		if (errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded)) && ctx.Err() == nil {
			return nil
		}

		return err
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, sub := range n.subscriptions[notification.Channel] {
		select {
		case sub.listenChan <- []byte(notification.Payload):
		default:
			n.logger.Error("dropped notification due to full buffer", "payload", notification.Payload)
		}
	}

	return nil
}

func (n *notifier) unlisten(ctx context.Context, sub *subscription) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	subs := n.subscriptions[sub.channel]

	// stop listening if last subscriber
	if len(subs) <= 1 {
		// UNLISTEN for this channel
		n.listenerUnlisten(ctx, sub.channel)
	}

	// remove subscription from the subscriptions map
	n.subscriptions[sub.channel] = slices.DeleteFunc(n.subscriptions[sub.channel], func(s *subscription) bool {
		return s == sub
	})
	if len(n.subscriptions[sub.channel]) < 1 {
		delete(n.subscriptions, sub.channel)
	}
	n.logger.Debug("removed subscription", "new_num_subscriptions", len(n.subscriptions[sub.channel]), "channel", sub.channel)

	return nil
}

func (n *notifier) Run(ctx context.Context) error {
	for {
		err := n.waitOnce(ctx)
		if err != nil || ctx.Err() != nil {
			return err
		}
	}
}
