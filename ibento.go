// Copyright (c) 2022 Zaba505
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

// Package ibento
package ibento

import (
	"context"
	"encoding/binary"
	"fmt"

	cloudevent "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	eventpb "github.com/cloudevents/sdk-go/binding/format/protobuf/v2/pb"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Log represents an readonly and append-only log of events
type Log struct {
	badger *badger.DB
	seq    *badger.Sequence
	log    *zap.Logger
}

type logOptions struct {
	bopts  badger.Options
	logger *zap.Logger
}

// Option is a type which helps override the default Log configuration.
type Option func(*logOptions)

// WithLogger
func WithLogger(logger *zap.Logger) Option {
	return func(lo *logOptions) {
		lo.logger = logger
	}
}

// Open opens a new event log.
func Open(dir string, opts ...Option) (*Log, error) {
	defaultOpts := logOptions{
		bopts:  badger.DefaultOptions(dir),
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(&defaultOpts)
	}

	badgerOpts := defaultOpts.bopts
	badgerOpts.Logger = badgerLogger{zap: defaultOpts.logger.Sugar()}
	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}

	seq, err := db.GetSequence([]byte("event_idx"), 1)
	if err != nil {
		return nil, err
	}

	l := &Log{
		badger: db,
		seq:    seq,
		log:    defaultOpts.logger,
	}
	return l, nil
}

// Close
func (l *Log) Close() error {
	l.seq.Release()
	return l.badger.Close()
}

// ValidationError represents that an invalid parameter
// was provided to a method or function.
type ValidationError struct {
	Cause error
}

// Error implements the error interface.
func (e ValidationError) Error() string {
	return fmt.Sprintf("invalid parameter provided: %s", e.Cause)
}

// Unwrap
func (e ValidationError) Unwrap() error {
	return e.Cause
}

const eventIdxKeyPrefix = "eventidx"

// Append will add the given event to the end of a Log.
func (l *Log) Append(ctx context.Context, ev event.Event) error {
	err := ev.Validate()
	if err != nil {
		l.log.Error("attempted to append an invalid event", zap.Error(err))
		return ValidationError{
			Cause: err,
		}
	}

	idx, err := l.seq.Next()
	if err != nil {
		l.log.Error("failed to allocate new event index in log", zap.Error(err))
		return err
	}
	var key [16]byte
	copy(key[:8], eventIdxKeyPrefix)
	binary.BigEndian.PutUint64(key[8:], idx)

	pb, err := cloudevent.ToProto(&ev)
	if err != nil {
		l.log.Error("failed to map cloudevent struct to protobuf message", zap.Error(err))
		return err
	}
	value, err := proto.Marshal(pb)
	if err != nil {
		l.log.Error("failed to marshal protobuf message to binary", zap.Error(err))
		return err
	}

	return l.badger.Update(func(txn *badger.Txn) error {
		return txn.Set(key[:], value)
	})
}

// Iterator
type Iterator struct {
	badger   *badger.DB
	iterOpts badger.IteratorOptions

	log *zap.Logger
}

// Iterator initializes a new iterator over the event log.
func (l *Log) Iterator() *Iterator {
	iterOpts := badger.DefaultIteratorOptions
	iterOpts.Prefix = []byte(eventIdxKeyPrefix)
	return &Iterator{
		badger:   l.badger,
		log:      l.log,
		iterOpts: iterOpts,
	}
}

// Consume
func (it *Iterator) Consume(f func(*event.Event) error) error {
	return it.badger.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(it.iterOpts)
		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			var cev eventpb.CloudEvent
			item := iter.Item()
			err := item.Value(func(val []byte) error {
				return cloudevent.DecodeData(context.TODO(), val, &cev)
			})
			if err != nil {
				it.log.Error("failed to unmarshal protobuf cloudevent", zap.Error(err))
				return err
			}

			ev, err := cloudevent.FromProto(&cev)
			if err != nil {
				it.log.Error(
					"failed to convert protobuf cloudevent to sdk cloudevent model",
					zap.String("event_id", cev.Id),
					zap.String("event_type", cev.Type),
					zap.String("event_source", cev.Source),
					zap.Error(err),
				)
				return err
			}

			err = f(ev)
			if err != nil {
				it.log.Error(
					"failed to apply given function to cloudevent",
					zap.String("event_id", cev.Id),
					zap.String("event_type", cev.Type),
					zap.String("event_source", cev.Source),
					zap.Error(err),
				)
				return err
			}
		}
		return nil
	})
}
