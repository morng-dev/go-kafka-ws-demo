package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

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

type HeartBeatManager struct {
	mu                 sync.RWMutex
	nodeID             string
	hub                ChatHub
	kafkaWriter        *kafka.Writer
	haartbeartReader   *kafka.Reader
	nodeLastSeen       map[string]int64
	processedDeadNodes map[string]bool
	nodeUsers          map[string][]string
	heartbeatIntervel  time.Duration
	timeoutThershould  time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
}

func NewHeartbeatManager(kafkaAddr, nodeID string, hub ChatHub) (*HeartBeatManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	km := NewKafkaManager(kafkaAddr)
	if err := km.EnsureTopics([]string{"node-heartbeat"}); err != nil {
		log.Printf("warning: could not ensure heartbeat topic exists: %v", err)
	}

	time.Sleep(1 * time.Second)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaAddr),
		Topic:                  "node-heartbeat",
		Balancer:               &kafka.Hash{},
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaAddr},
		Topic:          "node-heartbeat",
		GroupID:        "heartbeat-monitor-" + nodeID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	hm := &HeartBeatManager{
		nodeID:             nodeID,
		hub:                hub,
		kafkaWriter:        writer,
		haartbeartReader:   reader,
		nodeLastSeen:       make(map[string]int64),
		nodeUsers:          make(map[string][]string),
		processedDeadNodes: make(map[string]bool),
		heartbeatIntervel:  5 * time.Second,
		timeoutThershould:  15 * time.Second,
		ctx:                ctx,
		cancel:             cancel,
	}
	log.Printf("[%s] rebuilding node state from heartbeat", nodeID)
	if err := hm.rebuildNodeStateFromAllPartition(kafkaAddr); err != nil {
		log.Printf("[%s] node state rebuilding has issues: %v", nodeID, err)
	}
	//send heartbeat
	//listenToHeartbeat
	//monitorNodeHealth
	//cleanupOldDeadNodes
	log.Printf("heartbeat manager initazed for node %s", nodeID)
	return hm, nil
}

func (hm *HeartBeatManager) rebuildNodeStateFromAllPartition(kafkaAddr string) error {
	log.Printf("[%s] reading all heartbeat message from all partiton...", hm.nodeID)
	totalProcessed, err := ReadAllPartitions(kafkaAddr, "node-heartbeat", func(msg kafka.Message) error {
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return nil
		}
		if event.Type == "node_heartbeat" {
			var heartbeat NodeHeartBeat
			if err := json.Unmarshal(event.Data, &heartbeat); err != nil {
				return nil
			}
			hm.mu.Lock()
			hm.nodeLastSeen[heartbeat.NodeID] = heartbeat.TimeStamp
			hm.nodeUsers[heartbeat.NodeID] = heartbeat.UserID
			hm.mu.Unlock()
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("[%s] node state rebuilding complate : %d total heartbeat", hm.nodeID, totalProcessed)
	hm.mu.RLock()
	for nodeID, LastSeen := range hm.nodeLastSeen {
		users := hm.nodeUsers[nodeID]
		log.Printf("[%s] know node : %s (last need: %d, user: %d)", hm.nodeID, nodeID, LastSeen, len(users))
	}
	hm.mu.Unlock()
	return nil
}
