package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type CacheService interface {
	Ping() error
	Put(key string, value string) error
	Get(key string) (string, error)
	GetCluster() *redis.ClusterClient
}

type service struct {
	cluster *redis.ClusterClient
}

func NewService(hostPort string, password string) CacheService {
	if hostPort == "" {
		return nil
	}
	addr := strings.Split(hostPort, ",")
	var cluster *redis.ClusterClient
	if len(addr) > 1 {
		cluster = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: addr,
		})
	} else {
		cmd := redis.NewClient(&redis.Options{
			Addr:     hostPort,
			Password: password,
		})

		slots, err := cmd.ClusterSlots(context.Background()).Result()
		if err != nil {
			panic(err)
		}

		var addrs []string
		for _, slot := range slots {
			for _, node := range slot.Nodes {
				addrs = append(addrs, node.Addr)
			}
		}

		cluster = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			Password:     password,
			PoolSize:     10,
			PoolTimeout:  30 * time.Second,
			MinIdleConns: 2,
			IdleTimeout:  300 * time.Second,
		})
	}

	return &service{
		cluster: cluster,
	}
}

func (s *service) Ping() error {
	_, err := s.cluster.Ping(context.Background()).Result()
	if err == nil {
		fmt.Println("Redis cluster is available")
	} else {
		fmt.Println("Can't connect redis cluster", err)
	}
	return err
}

func (s *service) Put(key string, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.cluster.Set(ctx, key, value, 5*time.Minute).Err()
}

func (s *service) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := s.cluster.Get(ctx, key).Result()
	return r, err
}

func (s *service) GetCluster() *redis.ClusterClient {
	return s.cluster
}
