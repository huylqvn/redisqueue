package queue

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

type Queue struct {
	cluster   *redis.ClusterClient
	queueName string
	data      chan string
	quit      chan bool
}

func NewQueue(cluster *redis.ClusterClient, queueName string) *Queue {
	return &Queue{
		cluster:   cluster,
		data:      make(chan string),
		quit:      make(chan bool),
		queueName: queueName,
	}
}

func (q *Queue) Close() {
	q.quit <- true
	close(q.data)
}

func (q *Queue) GetLenQueue() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return q.cluster.LLen(ctx, q.queueName).Result()
}

func (q *Queue) Producer(data string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log.Println("Added: " + data)
	return q.cluster.LPush(ctx, q.queueName, data).Err()
}

func (q *Queue) PopQueue(ctx context.Context) {
	rs := q.cluster.BRPop(ctx, 0, q.queueName).Val()
	q.data <- rs[1]
	log.Println("Poped: " + rs[1])
}

func (q *Queue) Consumer(ctx context.Context, fn func(ctx context.Context, data string) error) {
	for {
		select {
		case <-q.quit:
			return
		default:
			go q.PopQueue(ctx)
			err := fn(ctx, <-q.data)
			if err != nil {
				log.Println("HandleConsumerErr", err)
				return
			}
		}
	}
}
