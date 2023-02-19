// Copyright (c) 2023 Zaba505
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package ibento

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Notification represents that an event has been appended to the Log.
type Notification struct {
	EventId     string
	EventSource string
	EventType   string
}

var errQueueClosed = errors.New("queue is closed")

type queue struct {
	eg *errgroup.Group

	closed  chan struct{}
	notifCh chan *Notification

	subIdx uint64
	subCh  chan func(map[uint64]*subscription)
}

func newQueue() *queue {
	g, gctx := errgroup.WithContext(context.Background())
	q := &queue{
		eg:      g,
		closed:  make(chan struct{}, 1),
		notifCh: make(chan *Notification),
		subCh:   make(chan func(map[uint64]*subscription)),
	}

	go q.run(gctx)

	return q
}

func (q *queue) run(ctx context.Context) {
	var subsMu sync.RWMutex
	subs := make(map[uint64]*subscription)
	q.eg.Go(func() error {
		for {
			var f func(map[uint64]*subscription)
			select {
			case <-ctx.Done():
				subsMu.Lock()
				for id := range subs {
					s := subs[id]
					close(s.closed)
					subs[id] = nil
				}
				subsMu.Unlock()
				return nil
			case <-q.closed:
				subsMu.Lock()
				for id := range subs {
					s := subs[id]
					close(s.closed)
					subs[id] = nil
				}
				subsMu.Unlock()
				return nil
			case f = <-q.subCh:
			}

			subsMu.Lock()
			f(subs)
			subsMu.Unlock()
		}
	})

	q.eg.Go(func() error {
		for {
			var n *Notification
			select {
			case <-ctx.Done():
				return nil
			case <-q.closed:
				return nil
			case n = <-q.notifCh:
			}
			if n == nil {
				return nil
			}

			subsMu.RLock()
			for _, sub := range subs {
				select {
				case <-ctx.Done():
				case <-q.closed:
				case sub.notifCh <- n:
				}
			}
			subsMu.RUnlock()
		}
	})
}

func (q *queue) Close() error {
	select {
	case <-q.closed:
		return errQueueClosed
	default:
	}
	close(q.closed)
	return q.eg.Wait()
}

func (q *queue) publish(ctx context.Context, n Notification) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.closed:
		return errQueueClosed
	case q.notifCh <- &n:
		return nil
	}
}

type subscription struct {
	closed  chan struct{}
	id      uint64
	notifCh chan *Notification
}

func (q *queue) subscribe() (*subscription, error) {
	select {
	case <-q.closed:
		return nil, errQueueClosed
	default:
	}

	id := atomic.AddUint64(&q.subIdx, 1)
	s := &subscription{
		closed:  make(chan struct{}, 1),
		id:      id,
		notifCh: make(chan *Notification),
	}
	f := func(m map[uint64]*subscription) {
		m[id] = s
	}
	select {
	case <-q.closed:
		return nil, errQueueClosed
	case q.subCh <- f:
	}
	return s, nil
}

func (s *subscription) recv(ctx context.Context) (*Notification, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.closed:
		return nil, errQueueClosed
	case n := <-s.notifCh:
		return n, nil
	}
}
