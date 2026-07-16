package kafka

import (
	"context"
	"encoding/json"
	"fmt"
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
	kafkaWriter   *kafka.Writer
	messageReader *kafka.Reader
	MessageCache  *MessageCache
	handler       MessageHandler
	ctx           context.Context
	cancel        context.CancelFunc
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
	mm := &MessageManager{
		kafkaWriter:   writer,
		messageReader: reader,
		MessageCache:  NewMessageCache(),
		handler:       handler,
		ctx:           ctx,
		cancel:        cancel,
	}
	go mm.listenToMessages()
	log.Println("Message Manager Initialized")
	return mm, nil
}

func (mm *MessageManager) PublishMessage(msg *Message) error {
	msg.MessageID = fmt.Sprintf("%s-%s-%d", msg.FromUserID, msg.ToUserID, msg.Timestamp)

	dataBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	event := Event{
		Type: "message",
		Data: json.RawMessage(dataBytes),
	}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conKey := GetCovKey(msg.FromUserID, msg.ToUserID)
	kafkaMsg := kafka.Message{
		Key:   []byte(conKey),
		Value: eventBytes,
		Topic: "chat-message",
	}
	return mm.kafkaWriter.WriteMessages(ctx, kafkaMsg)
}

func (mm *MessageManager) SendMessageWithRetry(msg *Message, maxRetries int) error {
	msg.Timestamp = time.Now().Unix()

	for i := 0; i < maxRetries; i++ {
		err := mm.PublishMessage(msg)
		if err == nil {
			return nil
		}
		log.Printf("Retry %d faild for message: %v", i+1, err)
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	return fmt.Errorf("faild to sent message after %d retry", maxRetries)
}

func (mm *MessageManager) listenToMessages() {
	log.Println("chat message kafka listener started")

	for {
		select {
		case <-mm.ctx.Done():
			log.Println("chat message listener stopped")
			return
		default:
		}
		msg, err := mm.messageReader.ReadMessage(mm.ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			log.Printf("kafka chat read error: %v, retring in 1s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		var event Event

		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error unmarshaling event : %v", err)
			continue
		}
		if event.Type == "message" {
			var chatMeg Message
			if err := json.Unmarshal(event.Data, &chatMeg); err != nil {
				log.Printf("Error unmarshal message: %v")
			}
			if !mm.MessageCache.Add(chatMeg.MessageID) {
				log.Printf("Duplicated message Ignored:%s", chatMeg.MessageID)
				continue
			}
			log.Printf("Process new message from %s ot %s", chatMeg.FromUserID, chatMeg.ToUserID)
			mm.handler.DeliverMessage(&chatMeg)
		}
	}
}

// close colces the message manager
func (mm *MessageManager) Close() {
	log.Println("Stopping message manager..")
	mm.cancel()

	if mm.kafkaWriter != nil {
		mm.kafkaWriter.Close()
	}
	if mm.messageReader != nil {
		mm.messageReader.Close()
	}
	log.Println("message manager stopped")
}

// helper
func GetCovKey(userID1, userID2 string) string {
	if userID1 > userID2 {
		return userID2 + "-" + userID1
	}
	return userID1 + "-" + userID2
}
