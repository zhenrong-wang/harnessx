package signalx

import (
	"context"
	"os/signal"
	"syscall"
)

func Context(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
}
