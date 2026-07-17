package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/morng-dev/go-kafka-ws-demo/kafka"
	"github.com/morng-dev/go-kafka-ws-demo/realtime"
)

func main() {
	nodeID := flag.String("node", "node-1", "not id for this instance")
	port := flag.String("port", "3000", "port to listen on")
	kafkaAddr := flag.String("kafka", "127.0.0.1:29092", "kafka address")
	flag.Parse()
	//wait for kafka to be ready

	if err := kafka.WaitForKafka(*kafkaAddr, 30*time.Second); err != nil {
		log.Fatalf("kafka nor ready:%d", err)
	}

	// app
	app := fiber.New(fiber.Config{
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})

	app.Use(logger.New())
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-type, Accept",
	}))

	//create chat hub
	chatHub, err := realtime.NewChatHub(*kafkaAddr, *nodeID)
	if err != nil {
		log.Fatalf("fail create Chat hub :%v", err)
	}

	//create heartbeat manager
	heartbeatMGR, err := kafka.NewHeartbeatManager(*kafkaAddr, *nodeID, chatHub)
	if err != nil {
		log.Fatalf("fail crate heartbeat manager: %v", err)
	}
	//========== chat route ===========//
	app.Get("/ws/:userID", chatHub.HandleClient)

	app.Get("/api/users/online", func(c *fiber.Ctx) error {
		users := chatHub.GetOnlineUser()
		return c.JSON(users)
	})

	//==== graful shutdown ====///
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGALRM)

	go func() {
		<-sigchan
		log.Println("shutting down graacfully...")

		heartbeatMGR.Stop()
		app.ShutdownWithTimeout(10 * time.Second)
	}()

	log.Printf("server starting on :%s", *port)
	if err := app.Listen(":" + *port); err != nil {
		log.Fatal(err)
	}
}
