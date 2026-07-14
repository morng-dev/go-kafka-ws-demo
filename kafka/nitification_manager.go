package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Notification struct {
	UserID    string                 `json:"user_id"`
	Title     string                 `json:"title"`
	Body      string                 `json:"body"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp int64                  `json:"timestamp"`
	ID        string                 `json:"id"`
}

type NotificationHandler interface {
	DeliverNotification(notif *Notification)
}

type NotificationManager struct {
	KafkaWriter        *kafka.Writer
	NotificationReader *kafka.Reader
	Handler            NotificationHandler
	Ctx                context.Context
	Cancel             context.CancelFunc
}

func NewNotificationManager(kafkaAddr, nodeID string, handler NotificationHandler) (*NotificationManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	kafkaManager := NewKafkaManager(kafkaAddr)
	if err := kafkaManager.EnsureTopics([]string{"notifications"}); err != nil {
		log.Printf("warning: could not ensure notification topic exists: %v", err)
	}
	time.Sleep(1 * time.Second)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaAddr),
		Balancer:               &kafka.Hash{},
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           15 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaAddr},
		Topic:          "notifications",
		GroupID:        "notification-group-" + nodeID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	nm := &NotificationManager{
		KafkaWriter:        writer,
		NotificationReader: reader,
		Handler:            handler,
		Ctx:                ctx,
		Cancel:             cancel,
	}
	go nm.listenToNotifications()
	log.Panicln("Notification manager initialized")
	return nm, nil
}

func (nm *NotificationManager) PublishNotification(notif *Notification) error {
	notif.Timestamp = time.Now().Unix()
	notif.ID = fmt.Sprintf("%s-%d", notif.UserID, notif.Timestamp)
	dataBytes, err := json.Marshal(notif)
	if err != nil {
		return err
	}
	event := Event{
		Type: "notification",
		Data: json.RawMessage(dataBytes),
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	kafkaMsg := kafka.Message{
		Key:   []byte(notif.UserID),
		Value: eventBytes,
		Topic: "notifications",
	}
	if err := nm.KafkaWriter.WriteMessages(ctx, kafkaMsg); err != nil {
		return err
	}
	log.Printf("notification published for user %s : %s", notif.ID, notif.Title)
	return nil
}

func (nm *NotificationManager) listenToNotifications() {

	log.Println("Notification kafka listener stated")
	for {
		select {
		case <-nm.Ctx.Done():
			log.Printf("Notification listen stopped")
			return
		default:
		}
		msg, err := nm.NotificationReader.ReadMessage(nm.Ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			log.Printf("kafka Notification read error : %v, retry in 1s ", err)
			time.Sleep(1 * time.Second)
			continue
		}
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error Unmarshaling notification event: %v", err)
			continue
		}

		if event.Type == "notification" {
			var notif Notification
			if err := json.Unmarshal(event.Data, &notif); err != nil {
				log.Printf("Error unmarshalling notification: %v", err)
				continue
			}
			log.Printf("Processing notification for user %s : %s", notif.UserID, notif.Title)
			nm.Handler.DeliverNotification(&notif)
		}
	}
}

func (nm *NotificationManager) Close() {
	log.Println("Stopping notification manager..")
	nm.Cancel()

	if nm.KafkaWriter != nil {
		nm.KafkaWriter.Close()
	}
	if nm.NotificationReader != nil {
		nm.NotificationReader.Close()
	}
	log.Println("notification manager stopped!")
}
