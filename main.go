package main

import (
	"context"
	"redis_protector/amdin"
	"redis_protector/redis"
	"redis_protector/server"
	"redis_protector/signal"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	controllerServer := server.NewRedisServer(ctx, redis.GetMasterClient(), redis.GetSlaveClient())
	adminServer := amdin.NewAdminServer(7666, controllerServer)
	controllerServer.Start()
	adminServer.Start()
	<-signal.Exit()
	cancel()
	time.Sleep(time.Second * 3)
}
