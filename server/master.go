package server

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"sync"
	"time"
)

const (
	queuePrefix    = "queue_*"
	ExpectQueueLen = 5
)

type ReportData struct {
	queueName string
	masterLen int
	slaveLen  int
}

type QueueStat struct {
	QueueName string
	ExpectLen int
	MasterLen int
	SlaveLen  int
}

type Server interface {
	Start()
	GetReport() []QueueStat
}

type RedisServer struct {
	sync.RWMutex
	ctx            context.Context
	cancelFunc     context.CancelFunc
	stats          map[string]*QueueStat
	workers        map[string]Worker
	masterRedisCli *redis.Client
	slaveRedisCli  *redis.Client
	reportDataChan chan ReportData
}

func NewRedisServer(parent context.Context, masterRedisCli, slaveRedisCli *redis.Client) *RedisServer {
	ctx, cancel := context.WithCancel(parent)
	return &RedisServer{
		ctx:            ctx,
		cancelFunc:     cancel,
		stats:          make(map[string]*QueueStat),
		workers:        make(map[string]Worker),
		masterRedisCli: masterRedisCli,
		slaveRedisCli:  slaveRedisCli,
		reportDataChan: make(chan ReportData, 100),
	}
}

func (s *RedisServer) Start() {
	go s.scan()
	go s.stat()
}

func (s *RedisServer) stat() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case data := <-s.reportDataChan:
			stat, ok := s.stats[data.queueName]
			if !ok {
				continue
			}
			s.Lock()
			stat.MasterLen = data.masterLen
			stat.SlaveLen = data.slaveLen
			s.Unlock()
		}
	}
}

func (s *RedisServer) scan() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Second * 2): //扫描间隔 10s
			queues, err := s.masterRedisCli.Keys(queuePrefix).Result()
			if err != nil {
				log.Printf("扫描队列错误%#v\n", err)
				continue
			}
			tmpMap := make(map[string]struct{})
			//增加
			for _, q := range queues {
				tmpMap[q] = struct{}{}
				if _, ok := s.stats[q]; !ok {
					s.Lock()
					s.stats[q] = &QueueStat{ExpectLen: ExpectQueueLen, QueueName: q}
					s.Unlock()
					worker := NewRedis2RedisWorker(s.ctx, s.masterRedisCli, s.slaveRedisCli, q,
						s.reportDataChan, make(chan struct{}), ExpectQueueLen)
					s.workers[q] = worker
					go worker.Start()
				}
			}
			//删除
			for k, _ := range s.stats {
				if _, ok := tmpMap[k]; !ok {
					fmt.Println("删除。。。。。。。。。。。。")
					s.workers[k].Stop()
					s.Lock()
					delete(s.stats, k)
					s.Unlock()
					delete(s.workers, k)
					s.slaveRedisCli.Del(k)
				}
			}
		}
	}
}

func (s *RedisServer) GetReport() []QueueStat {
	s.RLock()
	defer s.RUnlock()
	stats := make([]QueueStat, 0, len(s.stats))
	for _, v := range s.stats {
		stats = append(stats, *v)
	}
	return stats
}
