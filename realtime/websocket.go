package realtime

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/morng-dev/go-kafka-ws-demo/kafka"
)

func (h *ChatHub) HandleClient(c *fiber.Ctx) error {
	if websocket.IsWebSocketUpgrade(c) {
		userID := c.Params("userID")
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
					log.Printf("cleanup connection for user %s", userID)
					h.unregisterClint(userID)
					conn.Close()
					cancel()
				}
			}
			defer cleanup()

			h.sendInitalState(client)
			h.handleIncomeWebsocketMessage(ctx, client, cleanup)
			go h.handlePingPong(ctx, client, cleanup)
			go h.handleOutgoingWebsocketMessages(ctx, client, cleanup)

		})(c)
	}
	return fiber.ErrUpgradeRequired
}

func (h *ChatHub) sendInitalState(client *Client) {
	onlineusers := h.GetOnlineUser()

	initalState := map[string]interface{}{
		"type": "initial_state",
		"data": map[string]interface{}{
			"online_user": onlineusers,
		},
	}
	if err := client.Conn.WriteJSON(initalState); err != nil {
		log.Printf("Error sending iniial state to user %s : %v", client.UserID, err)
	} else {
		log.Printf("Sent initial state to %s : %d online users", client.UserID, len(onlineusers))
	}
}

func (h *ChatHub) handlePingPong(ctx context.Context, client *Client, cleanup func()) {
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

func (h *ChatHub) handleIncomeWebsocketMessage(ctx context.Context, client *Client, cleanup func()) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var msg kafka.Message
			if err := client.Conn.ReadJSON(&msg); err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("Websocket read error for user %s %v", client.UserID, err)
				}
				cleanup()
				return
			}
			msg.FromUserID = client.UserID
			if err := h.SendMessageWithRetry(&msg, 3); err != nil {
				log.Printf("Error sending message from user %s %v", client.UserID, err)
				client.Conn.WriteJSON(map[string]interface{}{
					"type": "error",
					"data": "faild to send message",
				})
			}
		}
	}
}

func (h *ChatHub) handleOutgoingWebsocketMessages(ctx context.Context, client *Client, cleanup func()) {
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
