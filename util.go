package divider

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"log/slog"
)

// StringToIntHash takes in a string and converts it to the md5-hashed version of it.
// It then converts that hash to a uin64,
// This value is then distributed between -2^53 and 2^53 (redis zset min/max)
func StringToIntHash(in string) int64 {
	s := md5.Sum([]byte(in))
	u := binary.BigEndian.Uint64(s[:])
	return shrink(u, 53)
}

// in: the input number that I want to cut down to space.
// bits: the bit count of the minimum and maximum that I want
// ie -2^53 - 2^53 would put in 53
func shrink(in uint64, bits int) int64 {
	return int64((in & ((2 << uint64(bits)) - 1))) - (2 << uint64(bits-1))
}

func Map[a any, b any](data []a, f func(a) b) []b {
	out := make([]b, len(data))
	for i, v := range data {
		out[i] = f(v)
	}
	return out
}

type LoggerGen func(context.Context) Logger
type Logger interface {
	Debug(msg string, args ...slog.Attr)
	Info(msg string, args ...slog.Attr)
	Error(msg string, args ...slog.Attr)
	Panic(msg string, args ...slog.Attr)
}

func DefaultLogger(ctx context.Context) Logger {
	return &defaultLogger{ctx: ctx}
}

type defaultLogger struct {
	ctx context.Context
}

func (d *defaultLogger) Debug(msg string, args ...slog.Attr) {
	l := slog.Default()
	for _, v := range args {
		l = l.With(v)
	}

	l.DebugContext(d.ctx, msg)
}
func (d *defaultLogger) Info(msg string, args ...slog.Attr) {
	l := slog.Default()
	for _, v := range args {
		l = l.With(v)
	}

	l.InfoContext(d.ctx, msg)
}
func (d *defaultLogger) Error(msg string, args ...slog.Attr) {
	l := slog.Default()
	for _, v := range args {
		l = l.With(v)
	}

	l.ErrorContext(d.ctx, msg)
}
func (d *defaultLogger) Panic(msg string, args ...slog.Attr) {
	l := slog.Default()
	for _, v := range args {
		l = l.With(v)
	}

	l.ErrorContext(d.ctx, msg)
	panic(msg)
}
