package server

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"math/rand"
	"time"
)

const (
	lowWatermarkFactor = 0.7
)

type Worker interface {
	Start()
	Stop()
	UpdateThreshold(newThreshold int)
}

type Redis2RedisWorker struct {
	ctx            context.Context
	masterRedisCli *redis.Client
	slaveRedisCli  *redis.Client
	queueName      string
	reportChan     chan<- ReportData
	endChan        chan struct{}
	thresholdChan  chan int
	threshold      int
	lowWaterMark   int
	upCount        int
	downCount      int
}

func NewRedis2RedisWorker(parent context.Context, masterRedisCli, slaveRedisCli *redis.Client,
	queueName string, reportChan chan<- ReportData, endChan chan struct{}, threshold int) *Redis2RedisWorker {
	ctx, _ := context.WithCancel(parent)
	return &Redis2RedisWorker{
		ctx:            ctx,
		masterRedisCli: masterRedisCli,
		slaveRedisCli:  slaveRedisCli,
		queueName:      queueName,
		reportChan:     reportChan,
		endChan:        endChan,
		threshold:      threshold,
		thresholdChan:  make(chan int, 1),
	}
}

func (w *Redis2RedisWorker) UpdateThreshold(newThreshold int) {
	w.thresholdChan <- newThreshold
}

func (w *Redis2RedisWorker) Start() {
	log.Println("worker start")
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.endChan:
			return
		case newThreshold := <-w.thresholdChan:
			w.threshold = newThreshold
			w.lowWaterMark = int(float64(w.threshold) * lowWatermarkFactor)
			w.upCount = 0
			w.downCount = 0
		case <-time.After(time.Second + time.Millisecond*time.Duration(rand.Intn(1000))): //5s - 10s
			masterLen, err := w.masterRedisCli.LLen(w.queueName).Result()
			if err != nil {
				if err == redis.Nil {
					log.Printf("Master队列%s被删除\n", w.queueName)
					w.slaveRedisCli.Del(w.queueName)
					return
				}
				log.Printf("获取Master队列%s长度错误\n,%#v\n", w.queueName, err)
				continue
			}
			slaveLen := w.slaveRedisCli.LLen(w.queueName).Val()
			//上报
			w.reportChan <- ReportData{
				queueName: w.queueName,
				masterLen: int(masterLen),
				slaveLen:  int(slaveLen),
			}
			log.Println("上报数据")
			if int(masterLen) > w.threshold {
				w.downCount = 0
				w.upCount++
				if w.upCount < 3 {
					continue
				}
				//取出
				mvLen := int(masterLen) - w.threshold
				buf := make([]interface{}, 0, mvLen)
				for i := 0; i < mvLen; i++ {
					result, err := w.masterRedisCli.LPop(w.queueName).Result()
					if err != nil {
						if err != redis.Nil {
							log.Printf("Lpop Master队列%s错误\n,%#v\n", w.queueName, err)
						}
						break
					}
					buf = append(buf, result)
				}
				if len(buf) > 0 {
					if err := w.slaveRedisCli.RPush(w.queueName, buf...).Err(); err != nil {
						log.Printf("RPush Slave队列%s错误,数据放回Master队列\n,%#v\n", w.queueName, err)
						w.masterRedisCli.RPush(w.queueName, buf...)
					} else {
						w.upCount = 0
					}

				}
			} else if int(masterLen) < w.lowWaterMark {
				w.upCount = 0
				w.downCount++
				if w.downCount < 3 {
					continue
				}
				//放回
				mvLen := w.threshold - int(masterLen)
				fmt.Println("需要放回的长度", mvLen)
				buf := make([]interface{}, 0, mvLen)
				for i := 0; i < mvLen; i++ {
					result, err := w.slaveRedisCli.LPop(w.queueName).Result()
					if err != nil {
						log.Printf("Lpop Slave队列%s错误\n,%#v\n", w.queueName, err)
						break
					}
					buf = append(buf, result)
				}
				if len(buf) > 0 {
					if err := w.masterRedisCli.RPush(w.queueName, buf...).Err(); err != nil {
						log.Printf("LPush Master队列%s错误,数据放回Slave队列\n,%#v\n", w.queueName, err)
						w.slaveRedisCli.RPush(w.queueName, buf...)
					} else {
						w.downCount = 0
					}

				}
			}
		}
	}
}

func (w *Redis2RedisWorker) Stop() {
	close(w.endChan)
}
