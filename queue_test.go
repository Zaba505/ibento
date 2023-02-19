// Copyright (c) 2023 Zaba505
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package ibento

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestQueue_Close(t *testing.T) {
	t.Run("will return an error", func(t *testing.T) {
		t.Run("if the queue is already closed", func(t *testing.T) {
			q := newQueue()
			err := q.Close()
			if !assert.Nil(t, err) {
				return
			}

			err = q.Close()
			if !assert.Equal(t, errQueueClosed, err) {
				return
			}
		})
	})
}

func TestQueue_publish(t *testing.T) {
	t.Run("will return an error", func(t *testing.T) {
		t.Run("if the context is cancelled", func(t *testing.T) {
			q := newQueue()
			defer q.Close()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := q.publish(ctx, Notification{})
			if !assert.Equal(t, context.Canceled, err) {
				return
			}
		})

		t.Run("if the queue is closed", func(t *testing.T) {
			q := newQueue()
			err := q.Close()
			if !assert.Nil(t, err) {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = q.publish(ctx, Notification{})
			if !assert.Equal(t, errQueueClosed, err) {
				return
			}
		})
	})

	t.Run("will publish a notification", func(t *testing.T) {
		t.Run("if the queue is not closed", func(t *testing.T) {
			q := &queue{
				closed:  make(chan struct{}, 1),
				notifCh: make(chan *Notification, 1),
			}
			defer close(q.closed)
			defer close(q.notifCh)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			n := Notification{
				EventId:     "1234",
				EventSource: "test",
				EventType:   "test",
			}
			err := q.publish(ctx, n)
			if !assert.Nil(t, err) {
				return
			}

			var n2 *Notification
			select {
			case <-ctx.Done():
				t.Fail()
				return
			case n2 = <-q.notifCh:
			}
			if !assert.Equal(t, n, *n2) {
				return
			}
		})
	})
}

func TestQueue_subscribe(t *testing.T) {
	t.Run("will return an error", func(t *testing.T) {
		t.Run("if the queue is already closed", func(t *testing.T) {
			q := newQueue()
			err := q.Close()
			if !assert.Nil(t, err) {
				return
			}

			_, err = q.subscribe()
			if !assert.Equal(t, errQueueClosed, err) {
				return
			}
		})
	})

	t.Run("will return a subscription", func(t *testing.T) {
		t.Run("if the queue is not closed", func(t *testing.T) {
			q := newQueue()
			defer q.Close()

			s, err := q.subscribe()
			if !assert.Nil(t, err) {
				return
			}
			if !assert.NotNil(t, s) {
				return
			}
		})
	})
}

func TestSubscription_recv(t *testing.T) {
	t.Run("will return an error", func(t *testing.T) {
		t.Run("if the context is cancelled", func(t *testing.T) {
			q := newQueue()
			defer q.Close()

			s, err := q.subscribe()
			if !assert.Nil(t, err) {
				return
			}
			if !assert.NotNil(t, s) {
				return
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err = s.recv(ctx)
			if !assert.Equal(t, context.Canceled, err) {
				return
			}
		})

		t.Run("if the queue is closed", func(t *testing.T) {
			q := newQueue()

			s, err := q.subscribe()
			if !assert.Nil(t, err) {
				return
			}
			if !assert.NotNil(t, s) {
				return
			}

			err = q.Close()
			if !assert.Nil(t, err) {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err = s.recv(ctx)
			if !assert.Equal(t, errQueueClosed, err) {
				return
			}
		})
	})

	t.Run("will return a notification", func(t *testing.T) {
		t.Run("if a notification has been published after subscribing", func(t *testing.T) {
			q := newQueue()
			defer q.Close()

			s, err := q.subscribe()
			if !assert.Nil(t, err) {
				return
			}
			if !assert.NotNil(t, s) {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			n := Notification{
				EventId:     "1234",
				EventSource: "test",
				EventType:   "test",
			}
			err = q.publish(ctx, n)
			if !assert.Nil(t, err) {
				return
			}

			n2, err := s.recv(ctx)
			if !assert.Nil(t, err) {
				return
			}
			if !assert.Equal(t, n, *n2) {
				return
			}
		})
	})

	t.Run("will not return a notification", func(t *testing.T) {
		t.Run("if a notification is published before subscribing", func(t *testing.T) {
			q := newQueue()
			defer q.Close()

			pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer pubCancel()

			n := Notification{
				EventId:     "1234",
				EventSource: "test",
				EventType:   "test",
			}
			err := q.publish(pubCtx, n)
			if !assert.Nil(t, err) {
				return
			}

			s, err := q.subscribe()
			if !assert.Nil(t, err) {
				return
			}
			if !assert.NotNil(t, s) {
				return
			}

			recvCtx, recvCancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer recvCancel()

			n2, err := s.recv(recvCtx)
			if !assert.Equal(t, context.DeadlineExceeded, err) {
				return
			}
			if !assert.Nil(t, n2) {
				return
			}
		})
	})
}

func BenchmarkQueue_publish_withNoSubscribers(b *testing.B) {
	q := newQueue()
	defer q.Close()

	b.RunParallel(func(pb *testing.PB) {
		n := Notification{
			EventId:     "1234",
			EventSource: "test",
			EventType:   "test",
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for pb.Next() {
			err := q.publish(ctx, n)
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkQueue_publish_withSubscribers_10(b *testing.B) {
	g, gctx := errgroup.WithContext(context.Background())
	defer func() {
		err := g.Wait()
		if err == nil {
			return
		}
		b.Error(err)
	}()

	q := newQueue()
	defer q.Close()

	for i := 0; i < 10; i++ {
		s, err := q.subscribe()
		if err != nil {
			b.Error(err)
			return
		}

		g.Go(func() error {
			for {
				_, err := s.recv(gctx)
				if err == errQueueClosed || err == context.Canceled {
					return nil
				}
				if err != nil {
					return err
				}
			}
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := Notification{
			EventId:     "1234",
			EventSource: "test",
			EventType:   "test",
		}
		ctx, cancel := context.WithCancel(gctx)
		defer cancel()

		for pb.Next() {
			err := q.publish(ctx, n)
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkQueue_publish_withSubscribers_100(b *testing.B) {
	g, gctx := errgroup.WithContext(context.Background())
	defer func() {
		err := g.Wait()
		if err == nil {
			return
		}
		b.Error(err)
	}()

	q := newQueue()
	defer q.Close()

	for i := 0; i < 100; i++ {
		s, err := q.subscribe()
		if err != nil {
			b.Error(err)
			return
		}

		g.Go(func() error {
			for {
				_, err := s.recv(gctx)
				if err == errQueueClosed || err == context.Canceled {
					return nil
				}
				if err != nil {
					return err
				}
			}
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := Notification{
			EventId:     "1234",
			EventSource: "test",
			EventType:   "test",
		}
		ctx, cancel := context.WithCancel(gctx)
		defer cancel()

		for pb.Next() {
			err := q.publish(ctx, n)
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}
