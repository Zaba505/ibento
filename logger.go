// Copyright (c) 2023 Zaba505
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package ibento

import "go.uber.org/zap"

type badgerLogger struct {
	zap *zap.SugaredLogger
}

func (l badgerLogger) Errorf(template string, args ...interface{}) {
	l.zap.Errorf(template, args...)
}

func (l badgerLogger) Warningf(template string, args ...interface{}) {
	l.zap.Warnf(template, args...)
}

func (l badgerLogger) Infof(template string, args ...interface{}) {
	l.zap.Infof(template, args...)
}

func (l badgerLogger) Debugf(template string, args ...interface{}) {
	l.zap.Debugf(template, args...)
}
