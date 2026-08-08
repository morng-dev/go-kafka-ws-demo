package realtime

import (
	"context"
	"log"

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

		})(c)
	}
	return fiber.ErrUpgradeRequired
}
