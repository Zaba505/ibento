// Copyright (c) 2022 Zaba505
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package ibento

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestOpen(t *testing.T) {
	t.Run("will return an error", func(t *testing.T) {
		t.Run("if given an empty directory", func(t *testing.T) {
			log, err := Open("")
			if !assert.Error(t, err) {
				return
			}
			if !assert.Equal(t, "Error Creating Dir: \"\" error: mkdir : no such file or directory", err.Error()) {
				t.Log(err)
				return
			}

			if !assert.Nil(t, log) {
				return
			}
		})
	})
}

func ExampleOpen() {
	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)

	log, err := Open(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer log.Close() // don't forget to close when done

	fmt.Println("opened")
	// Output: opened
}

func ExampleOpen_customOptions() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Println(err)
		return
	}

	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)

	log, err := Open(dir, WithLogger(logger))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer log.Close() // don't forget to close when done

	fmt.Println("opened")
	// Output: opened
}

func TestLog(t *testing.T) {
	t.Run("will not overwrite previously written events", func(t *testing.T) {
		t.Run("if the log is closed and reopened", func(t *testing.T) {
			openAndAppend := func(dir string, ev event.Event) error {
				log, err := Open(dir)
				if err != nil {
					return err
				}
				defer log.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				return log.Append(ctx, ev)
			}

			dir := t.TempDir()

			ev := event.New()
			ev.SetID("1234")
			ev.SetType("test")
			ev.SetSource("test1")
			err := openAndAppend(dir, ev)
			if !assert.Nil(t, err) {
				return
			}

			ev = event.New()
			ev.SetID("4321")
			ev.SetType("test")
			ev.SetSource("test2")
			err = openAndAppend(dir, ev)
			if !assert.Nil(t, err) {
				return
			}

			eventlog, err := Open(dir)
			if !assert.Nil(t, err) {
				return
			}
			defer eventlog.Close()

			var events []event.Event
			numOfEvents := 0
			err = eventlog.
				Iterator().
				Consume(func(e *event.Event) error {
					numOfEvents += 1
					events = append(events, *e)
					return nil
				})
			if !assert.Nil(t, err) {
				return
			}

			if !assert.Equal(t, 2, numOfEvents) {
				return
			}
			if !assert.Equal(t, events[0].ID(), "1234") {
				return
			}
			if !assert.Equal(t, events[1].ID(), "4321") {
				return
			}
		})
	})
}

func TestLog_Append(t *testing.T) {
	t.Run("will return an error", func(t *testing.T) {
		t.Run("if given an invalid cloudevent", func(t *testing.T) {
			dir := t.TempDir()

			log, err := Open(dir)
			if !assert.Nil(t, err) {
				return
			}
			if !assert.NotNil(t, log) {
				return
			}
			defer log.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = log.Append(ctx, event.New())
			if !assert.Error(t, err) {
				return
			}
			if !assert.IsType(t, ValidationError{}, err) {
				return
			}
		})

		t.Run("if log already been closed", func(t *testing.T) {
			dir := t.TempDir()

			log, err := Open(dir)
			if !assert.Nil(t, err) {
				return
			}
			if !assert.NotNil(t, log) {
				return
			}
			defer log.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			ev := event.New()
			ev.SetSource("example")
			ev.SetID("1")
			ev.SetType("example")
			err = log.Append(ctx, ev)
			if !assert.Nil(t, err) {
				return
			}

			err = log.Close()
			if !assert.Nil(t, err) {
				return
			}

			err = log.Append(ctx, ev)
			if !assert.Error(t, err) {
				return
			}
		})
	})
}

func ExampleLog_Append() {
	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)

	log, err := Open(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer log.Close() // don't forget to close when done

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ev := event.New()
	ev.SetSource("example")
	ev.SetID("1")
	ev.SetType("example")

	err = log.Append(ctx, ev)
	fmt.Println(err)
	// Output: <nil>
}

func BenchmarkLog_Append(b *testing.B) {
	dir := b.TempDir()
	log, err := Open(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer log.Close()

	ctx := context.Background()

	ev := event.New()
	ev.SetSource("example")
	ev.SetID("1")
	ev.SetType("example")

	for i := 0; i < b.N; i++ {
		err = log.Append(ctx, ev)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func ExampleLog_Iterator() {
	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)

	eventlog, err := Open(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer eventlog.Close() // don't forget to close when done

	ev1 := event.New()
	ev1.SetID("1234")
	ev1.SetType("test")
	ev1.SetSource("test")
	err = eventlog.Append(context.Background(), ev1)
	if err != nil {
		fmt.Println(err)
		return
	}

	ev2 := event.New()
	ev2.SetID("4321")
	ev2.SetType("test")
	ev2.SetSource("test")
	err = eventlog.Append(context.Background(), ev2)
	if err != nil {
		fmt.Println(err)
		return
	}

	numOfEvents := 0
	err = eventlog.
		Iterator().
		Consume(func(e *event.Event) error {
			numOfEvents += 1
			return nil
		})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(numOfEvents)
	// Output: 2
}
