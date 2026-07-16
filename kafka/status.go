package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type UserStatus struct {
	UserID    string `json:"user_id"`
	NodeID    string `json:"node_id"`
	Online    bool   `json:"online"`
	Timestamp int64  `json:"timestamp"`
}

type StatusHandler interface {
	HandlerUserStatus(status UserStatus)
}

type StatusManager struct {
	mu           sync.RWMutex
	nodeID       string
	kafkaWriter  *kafka.Writer
	statusReader *kafka.Reader
	onlineUser   map[string]UserStatus
	handler      StatusHandler
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewStatusMenager(kafkaAddr, nodeID string, handler StatusHandler) (*StatusManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	km := NewKafkaManager(kafkaAddr)
	if err := km.EnsureTopics([]string{"user-status"}); err != nil {
		log.Printf("Warning: could not ensure user-status topic exists: %v", err)
	}
	time.Sleep(1 * time.Second)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaAddr),
		Balancer:               &kafka.Hash{},
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaAddr},
		Topic:          "user-status",
		GroupID:        "status-group-" + nodeID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	sm := &StatusManager{
		nodeID:       nodeID,
		kafkaWriter:  writer,
		statusReader: reader,
		onlineUser:   make(map[string]UserStatus),
		handler:      handler,
		ctx:          ctx,
		cancel:       cancel,
	}
	// Rebuild state form all partition
	log.Printf("%s Rebuild user status form kafka cluser...", nodeID)
	if err := sm.rebuildStatusFromAllPartition(kafkaAddr); err != nil {
		log.Printf("%s status rebuilding has isses: %v", nodeID, err)
	}
	//go sm.listenToStatus()
	log.Printf("%s Status manager ready - %d users online", nodeID, sm.countOnlineUser())
	return sm, nil
}

func (sm *StatusManager) PublishStatus(status UserStatus) error {
	dataBytes, err := json.Marshal(status)
	if err != nil {
		return err
	}
	event := Event{
		Type: "user_status",
		Data: json.RawMessage(dataBytes),
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg := kafka.Message{
		Key:   []byte(status.UserID),
		Value: eventBytes,
		Topic: "user-status",
	}
	return sm.kafkaWriter.WriteMessages(ctx, msg)
}

func (sm *StatusManager) listenToStatus() {
	log.Println("User status kafka listener started")
	for {
		select {
		case <-sm.ctx.Done():
			log.Println("user status listener stopped")
			return
		default:
		}
		msg, err := sm.statusReader.ReadMessage(sm.ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			log.Printf("Kafka status read error: %v, retring in 1s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("error unmarshaling event: %v", err)
			continue
		}
		if event.Type == "user_status" {
			var status UserStatus
			if err := json.Unmarshal(event.Data, &status); err != nil {
				log.Printf("Error unmashaling user status: %v", err)
				continue
			}
			sm.mu.Lock()
			sm.onlineUser[status.UserID] = status
		}
	}
}

func (sm *StatusManager) rebuildStatusFromAllPartition(kafkaAddr string) error {
	log.Printf("%s Reading All User Status message form all Patitions...", sm.nodeID)
	totalProcessed := 0
	onlineCount := 0

	processed, err := ReadAllPartitions(kafkaAddr, "user_status", func(msg kafka.Message) error {
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return nil
		}
		if event.Type == "user_status" {
			var status UserStatus
			if err := json.Unmarshal(event.Data, &status); err != nil {
				return nil
			}

			sm.mu.Lock()
			sm.onlineUser[status.UserID] = status
			sm.mu.Unlock()

			if status.Online {
				onlineCount++
			}
		}
		return nil
	})
	totalProcessed = processed
	if err != nil {
		return err
	}

	log.Printf("%s state rebuilding complted :%d total message, %d online users", sm.nodeID, totalProcessed, onlineCount)
	sm.mu.RLock()
	for userID, status := range sm.onlineUser {
		if status.Online {
			log.Printf("%s online User : %s no node %s", sm.nodeID, userID, status.NodeID)
		}
	}
	sm.mu.RUnlock()
	return nil
}

func (sm *StatusManager) countOnlineUser() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	count := 0
	for _, status := range sm.onlineUser {
		if status.Online {
			count++
		}
	}
	return count
}
