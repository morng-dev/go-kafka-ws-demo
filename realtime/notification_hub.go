package realtime

import (
	"log"
	"sync"

	"github.com/gofiber/websocket/v2"
	"github.com/morng-dev/go-kafka-ws-demo/kafka"
)

type NotificationHub struct {
	mu                  sync.RWMutex
	clients             map[string]*NotificationClient
	nodeID              string
	notificationManager *kafka.NotificationManager
}

func NewNotificationHub(kafkaAddr, nodeID string) (*NotificationHub, error) {
	log.Printf("%s Initilizing notificationhub...", nodeID)
	hub := &NotificationHub{
		clients: make(map[string]*NotificationClient),
		nodeID:  nodeID,
	}
	notificationManager, err := kafka.NewNotificationManager(kafkaAddr, nodeID, hub)
	if err != nil {
		return nil, err
	}
	hub.notificationManager = notificationManager
	log.Printf("%s Notificatiuon Hub ready", nodeID)
	return hub, nil
}

func (h *NotificationHub) RegisterClient(userID string, conn *websocket.Conn) *NotificationClient {
	h.mu.Lock()
	defer h.mu.Unlock()

	client := &NotificationClient{
		UserID: userID,
		Conn:   conn,
		Send:   make(chan interface{}, 256),
		closed: false,
	}

	h.clients[userID] = client
	log.Printf("Notification User %s Connected on node %s", userID, h.nodeID)
	return client
}

func (h *NotificationHub) UnregisterClient(userID string, conn *websocket.Conn) *NotificationClient {
	h.mu.Lock()
	defer h.mu.Unlock()
	if client, ok := h.clients[userID]; ok {
		client.mu.Lock()
		if !client.closed {
			close(client.Send)
			client.closed = true
		}
		client.mu.Unlock()

		delete(h.clients, userID)
		log.Printf("Notification user %s dissconnected %s", userID, h.nodeID)
		return client
	}
	return nil
}

// DeliverNotification implements [kafka.NotificationHandler].
func (h *NotificationHub) DeliverNotification(notif *kafka.Notification) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	response := map[string]interface{}{
		"type": "notification",
		"data": notif,
	}

	if client, ok := h.clients[notif.UserID]; ok {
		client.mu.Lock()
		if !client.closed {
			select {
			case client.Send <- response:
				log.Printf("Notification Delivaerd to %s %s ", notif.UserID, notif.Title)
			default:
				log.Printf("Notification chanel full for user %s", notif.UserID)
			}
		}
		client.mu.Unlock()
	} else {
		log.Printf("Notification user %s not connected to this node", notif.UserID)
	}
}

func (h *NotificationHub) SendNotification(notif *kafka.Notification) error {
	return h.notificationManager.PublishNotification(notif)
}

func (h *NotificationHub) GetConnectedUser() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	user := make([]string, 0, len(h.clients))

	for userID := range h.clients {
		user = append(user, userID)
	}
	return user
}
