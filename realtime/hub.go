package realtime

import (
	"log"
	"sync"
	"time"

	"github.com/gofiber/websocket/v2"
	"github.com/morng-dev/go-kafka-ws-demo/kafka"
)

type ChatHub struct {
	mu             sync.RWMutex
	clients        map[string]*Client
	nodeID         string
	messageManager *kafka.MessageManager
	statusManager  *kafka.StatusManager
}

func NewChatHub(kafkaAddr, nodeID string) (*ChatHub, error) {
	log.Printf("%s Initializing chathub...", nodeID)

	hub := &ChatHub{
		clients: make(map[string]*Client),
		nodeID:  nodeID,
	}

	statusManager, err := kafka.NewStatusMenager(kafkaAddr, nodeID, hub)
	if err != nil {
		return nil, err
	}
	hub.statusManager = statusManager

	messageManager, err := kafka.NewMessageManager(kafkaAddr, nodeID, hub)
	if err != nil {
		statusManager.Close()
		return nil, err
	}
	hub.messageManager = messageManager
	log.Printf("%s chat hub redy whit %d online user", nodeID, len(hub.GetOnlineUser()))
	return hub, nil
}

func (h *ChatHub) GetOnlineUser() map[string]kafka.UserStatus {
	return h.statusManager.GetOnlineUser()
}

// HandlerUserStatus implements [kafka.StatusHandler].
func (h *ChatHub) HandlerUserStatus(status kafka.UserStatus) {
	log.Printf("User %s status: online=%v (node: %s)", status.UserID, status.Online, status.NodeID)
	h.broadcastToOthers(status.UserID, map[string]interface{}{
		"type": "user_status",
		"data": status,
	})
}

func (h *ChatHub) broadcastToOthers(excludeUserID string, data interface{}) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for userID, client := range h.clients {
		if userID != excludeUserID {
			client.mu.Lock()
			if !client.closed {
				select {
				case client.Send <- data:
					log.Printf("Brodcasted to user %s", userID)
				default:
					log.Printf("could not brodcast to user %s (channel) full", userID)
				}
			} else {
				log.Printf("Skiping brodcat to user %s (client closed)", userID)
			}
			client.mu.Unlock()
		}
	}
}

func (h *ChatHub) DeliverMessage(msg *kafka.Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	response := map[string]interface{}{
		"type": "message",
		"data": msg,
	}

	if recipient, ok := h.clients[msg.ToUserID]; ok {
		recipient.mu.Lock()
		if !recipient.closed {
			select {
			case recipient.Send <- response:
				log.Printf("message deliverd to %s", msg.ToUserID)
			default:
				log.Printf("Send chanel full for user %s", msg.ToUserID)
			}
		}
		recipient.mu.Unlock()
	}
	if sender, ok := h.clients[msg.FromUserID]; ok {
		sender.mu.Lock()
		if !sender.closed {
			select {
			case sender.Send <- response:
				log.Printf("Message confirmation sent to sender %s", msg.FromUserID)
			default:
				log.Printf("Send chanel full for sender %s", msg.FromUserID)
			}
		}
		sender.mu.Unlock()
	}
}

func (h *ChatHub) GetConnectedUserID() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	userIDs := make([]string, 0, len(h.clients))
	for userID := range h.clients {
		userIDs = append(userIDs, userID)
	}
	return userIDs
}

func (h *ChatHub) PublishUserStatus(userID, nodeID string, online bool) error {
	status := kafka.UserStatus{
		UserID:    userID,
		NodeID:    nodeID,
		Online:    online,
		Timestamp: time.Now().Unix(),
	}
	return h.statusManager.PublishStatus(status)
}

func (h *ChatHub) SendMessageWithRetry(msg *kafka.Message, maxRetries int) error {
	return h.messageManager.SendMessageWithRetry(msg, maxRetries)
}

func (h *ChatHub) RegisterClient(userID string, conn *websocket.Conn) *Client {
	h.mu.Lock()
	defer h.mu.Unlock()

	Client := &Client{
		UserID: userID,
		Conn:   conn,
		Send:   make(chan interface{}, 256),
		closed: false,
	}
	h.clients[userID] = Client

	status := kafka.UserStatus{
		UserID:    userID,
		NodeID:    h.nodeID,
		Online:    true,
		Timestamp: time.Now().Unix(),
	}

	go func() {
		if err := h.statusManager.PublishStatus(status); err != nil {
			log.Printf("Fail to publish online status for user %s : %v", userID, err)
		}
	}()
	log.Printf("User %s Connected on node %s", userID, h.nodeID)
	return Client
}

func (h *ChatHub) unregisterClint(userID string) {
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

		status := kafka.UserStatus{
			UserID:    userID,
			NodeID:    h.nodeID,
			Online:    false,
			Timestamp: time.Now().Unix(),
		}

		go func() {
			if err := h.statusManager.PublishStatus(status); err != nil {
				log.Printf("Fail to publish offline status for user %s : %v", userID, err)
			}
		}()
		log.Printf("User %s disconnected from node %s", userID, h.nodeID)
	}
}

func (h *ChatHub) Close() {
	log.Println("closing chat hub..")
	if h.messageManager != nil {
		h.messageManager.Close()
	}
	if h.statusManager != nil {
		h.statusManager.Close()
	}

	log.Printf("chat hub close")
}
