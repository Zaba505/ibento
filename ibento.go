// Copyright (c) 2022 Zaba505
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

// Package ibento
package ibento

import (
	"context"
	"fmt"
	"strconv"

	eventpb "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
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

	db, err := badger.Open(defaultOpts.bopts)
	if err != nil {
		return nil, err
	}

	seq, err := db.GetSequence([]byte("oneri"), ^uint64(0))
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

// Append will add the given event to the end of a Log.
func (l *Log) Append(ctx context.Context, ev event.Event) error {
	err := ev.Validate()
	if err != nil {
		l.log.Error("attempted to append an invalid event", zap.Error(err))
		return ValidationError{
			Cause: err,
		}
	}

	i, err := l.seq.Next()
	if err != nil {
		l.log.Error("failed to increment log pointer", zap.Error(err))
		return err
	}
	key := strconv.FormatUint(i, 10)

	pb, err := eventpb.ToProto(&ev)
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
		return txn.Set([]byte(key), value)
	})
}
