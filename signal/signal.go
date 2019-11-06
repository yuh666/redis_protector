package signal

import (
	"os"
	"os/signal"
	"syscall"
)

func Exit() <-chan os.Signal {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM)
	return c
}
