package kafka

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Message struct {
	FromUserID string `json:"from_user_id"`
	ToUserID   string `json:"to_user_id"`
	Content    string `json:"content"`
	Timestamp  int64  `json:"timestamp"`
	MessageID  string `json:"message_id,omitempty"`
}

type MessageCache struct {
	messages map[string]int64
	mu       sync.RWMutex
}

func NewMessageCache() *MessageCache {
	cache := &MessageCache{
		messages: make(map[string]int64),
	}
	go cache.cleanup()
	return cache
}

func (mc *MessageCache) Add(messageID string) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if _, exist := mc.messages[messageID]; exist {
		return false
	}
	mc.messages[messageID] = time.Now().Unix()
	return true
}

func (mc *MessageCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		mc.mu.Lock()
		now := time.Now().Unix()
		for id, ts := range mc.messages {
			if now-ts > 300 {
				delete(mc.messages, id)
			}
		}
		mc.mu.Unlock()
	}
}

type MessageHandler interface {
	DeliverMessage(msg *Message)
}

type MessageManager struct {
	kafkaWriter  *kafka.Writer
	kafkaReader  *kafka.Reader
	MessageCache *MessageCache
	handler      MessageHandler
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewMessageManager(kafkaAddr, nodeID string, handler MessageHandler) (*MessageManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	km := NewKafkaManager(kafkaAddr)
	if err := km.EnsureTopics([]string{"chat-message"}); err != nil {
		log.Printf("Warning: could not ensure chat-message toppic exists: %v", err)
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
		Topic:          "chat-message",
		GroupID:        "chat-group-" + nodeID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	messageMG := &MessageManager{
		kafkaWriter:  writer,
		kafkaReader:  reader,
		MessageCache: NewMessageCache(),
		handler:      handler,
		ctx:          ctx,
		cancel:       cancel,
	}

	log.Println("Message Manager Initialized")
	return messageMG, nil
}
