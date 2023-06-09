package queue

import (
	"context"
	"fmt"
	"time"
)

func Test() {
	cache := NewService("localhost:6379", "")
	queue := NewQueue(cache.GetCluster(), "test")
	go queue.Consumer(context.Background(), func(c context.Context, data string) error {
		fmt.Println(data)
		return nil
	})
	for i := 0; i < 10; i++ {
		queue.Producer(fmt.Sprintf("{%d}", i))
		time.Sleep(2 * time.Second)
	}
	queue.Stop()
}
