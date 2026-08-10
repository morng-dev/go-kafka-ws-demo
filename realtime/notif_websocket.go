package realtime

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
)

func (h *NotificationHub) HandleNotificationClient(c *fiber.Ctx) error {
	if websocket.IsWebSocketUpgrade(c) {
		userID := c.Locals("userID").(string)
		if userID == "" {
			return fiber.ErrBadRequest
		}
		return websocket.New(func(conn *websocket.Conn) {
			conn.SetReadLimit(8192)
			client := h.RegisterClient(userID, conn)
			ctx, cancel := context.WithCancel(context.Background())

			cleanupDone := false
			cleanup := func() {
				if !cleanupDone {
					cleanupDone = true
					log.Printf("Notification Cleaning up Conncetion for user %s", userID)
					h.UnregisterClient(userID, conn)
					conn.Close()
					cancel()
				}
			}
			defer cleanup()

			h.sendWelcomeMessage(client)
			go h.handlePingPong(ctx, client, cleanup)
			h.handleOutgoingNotifications(ctx, client, cleanup)
		})(c)
	}
	return fiber.ErrUpgradeRequired
}

func (h *NotificationHub) sendWelcomeMessage(client *NotificationClient) {
	wellcome := map[string]interface{}{
		"type": "wellcome",
		"data": map[string]interface{}{
			"message": "Conneted to notification",
			"user_id": client.UserID,
			"node_id": h.nodeID,
		},
	}
	if err := client.Conn.WriteJSON(wellcome); err != nil {
		log.Printf("Notification error sending welcome to %s %v", client.UserID, err)
	} else {
		log.Printf("notification wellcome sent to %s", client.UserID)
	}
}

func (h *NotificationHub) handlePingPong(ctx context.Context, client *NotificationClient, cleanup func()) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			client.mu.Lock()
			if client.closed {
				client.mu.Unlock()
				return
			}
			if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				client.mu.Unlock()
				cleanup()
				return
			}
			client.mu.Unlock()
		}
	}
}

func (h *NotificationHub) handleOutgoingNotifications(ctx context.Context, client *NotificationClient, cleanup func()) {
	defer cleanup()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-client.Send:
			if !ok {
				return
			}
			client.mu.Lock()
			if client.closed {
				client.mu.Unlock()
				return
			}
			if err := client.Conn.WriteJSON(data); err != nil {
				client.mu.Unlock()
				return
			}
			client.mu.Unlock()
		}
	}
}

func (h *NotificationHub) Close() {
	log.Println("Notification closing notification hub...")
	if h.notificationManager != nil {
		h.notificationManager.Close()
	}
	log.Println("notification closed")
}
