package redis

import (
	"fmt"
	"redis_protector/server"
	"testing"
	"time"
)

func TestGetQueues(t *testing.T) {
	client := GetMasterClient()
	result := client.Keys("queue").Val()
	fmt.Println(result)
}

func TestGetQueueLen(t *testing.T) {
	client := GetMasterClient()
	l := client.LLen("queue_1").Val()
	fmt.Println(l)
}

func TestRpush(t *testing.T) {
	client := GetMasterClient()
	var i int
	for {
		client.RPush(fmt.Sprintf("queue_%d", i%10), 1, 2, 3, 4)
		time.Sleep(time.Millisecond * 100)
		i++
	}
}

func TestLpop(t *testing.T) {
	client := GetMasterClient()
	var i int
	for {
		client.LPop(fmt.Sprintf("queue_%d", i%10))
		time.Sleep(time.Millisecond * 10)
		i++
	}

}

func TestDeref(t *testing.T) {
	s1 := server.QueueStat{}
	s2 := *(&s1)
	fmt.Printf("%p,%p", &s1, &s2)
}
