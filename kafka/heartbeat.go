package kafka

import (
	"sync"

	"github.com/segmentio/kafka-go"
)

type NodeHeartBeat struct {
	NodeID    string   `json:"node_id"`
	TimeStamp int64    `json:"timestamp"`
	UserID    []string `json:"user_id"`
}

type ChatHub interface {
	GetConnectedUserID() string
	PublishUserStatus(userID, nodeID string, online bool) error
	GetClientsLock() *sync.RWMutex
	GetClient() map[string]interface{}
}

type HeartBeat struct {
	mu               sync.RWMutex
	nodeID           string
	hub              ChatHub
	kafkaWriter      *kafka.Writer
	haartbeartReader *kafka.Reader
	nodeLastSeen     map[string]int64
}
