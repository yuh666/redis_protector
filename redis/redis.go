package redis

import (
	"github.com/go-redis/redis"
	"log"
)

var masterClient *redis.Client
var slaveClient *redis.Client

func init() {
	masterClient = redis.NewClient(&redis.Options{
		Addr: "localhost:5555",
	})

	if _, err := masterClient.Ping().Result(); err != nil {
		log.Fatal(err)
	}
}

func init() {
	slaveClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6666",
	})

	if _, err := masterClient.Ping().Result(); err != nil {
		log.Fatal(err)
	}
}

func GetMasterClient() *redis.Client {
	return masterClient
}

func GetSlaveClient() *redis.Client {
	return slaveClient
}
