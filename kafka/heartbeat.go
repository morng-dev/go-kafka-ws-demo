package kafka

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type NodeHeartBeat struct {
	NodeID    string   `json:"node_id"`
	TimeStamp int64    `json:"timestamp"`
	UserID    []string `json:"user_id"`
}

type ChatHub interface {
	GetConnectedUserID() []string
	PublishUserStatus(userID, nodeID string, online bool) error
	// GetClientsLock() *sync.RWMutex
	// GetClient() map[string]interface{}
}

type HeartBeatManager struct {
	mu                 sync.RWMutex
	nodeID             string
	hub                ChatHub
	kafkaWriter        *kafka.Writer
	heartbeartReader   *kafka.Reader
	nodeLastSeen       map[string]int64
	processedDeadNodes map[string]bool
	nodeUsers          map[string][]string
	heartbeatIntervel  time.Duration
	timeoutThershould  time.Duration
	ctx                context.Context
	cancel             context.CancelFunc
}

func NewHeartbeatManager(kafkaAddr, nodeID string, hub ChatHub) (*HeartBeatManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	km := NewKafkaManager(kafkaAddr)
	if err := km.EnsureTopics([]string{"node-heartbeat"}); err != nil {
		log.Printf("warning: could not ensure heartbeat topic exists: %v", err)
	}

	time.Sleep(1 * time.Second)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaAddr),
		Topic:                  "node-heartbeat",
		Balancer:               &kafka.Hash{},
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaAddr},
		Topic:          "node-heartbeat",
		GroupID:        "heartbeat-monitor-" + nodeID,
		StartOffset:    kafka.LastOffset,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	hm := &HeartBeatManager{
		nodeID:             nodeID,
		hub:                hub,
		kafkaWriter:        writer,
		heartbeartReader:   reader,
		nodeLastSeen:       make(map[string]int64),
		nodeUsers:          make(map[string][]string),
		processedDeadNodes: make(map[string]bool),
		heartbeatIntervel:  5 * time.Second,
		timeoutThershould:  15 * time.Second,
		ctx:                ctx,
		cancel:             cancel,
	}
	log.Printf("[%s] rebuilding node state from heartbeat", nodeID)
	if err := hm.rebuildNodeStateFromAllPartition(kafkaAddr); err != nil {
		log.Printf("[%s] node state rebuilding has issues: %v", nodeID, err)
	}
	//send heartbeat
	go hm.sendheatbeat()
	//listenToHeartbeat
	go hm.listenToHeartBeat()
	//monitorNodeHealth
	go hm.monitorNodeHealth()
	//cleanupOldDeadNodes
	go hm.cleanupOldDeadNodes()
	log.Printf("heartbeat manager initazed for node %s", nodeID)
	return hm, nil
}

func (hm *HeartBeatManager) rebuildNodeStateFromAllPartition(kafkaAddr string) error {
	log.Printf("[%s] reading all heartbeat message from all partiton...", hm.nodeID)
	totalProcessed, err := ReadAllPartitions(kafkaAddr, "node-heartbeat", func(msg kafka.Message) error {
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return nil
		}
		if event.Type == "node_heartbeat" {
			var heartbeat NodeHeartBeat
			if err := json.Unmarshal(event.Data, &heartbeat); err != nil {
				return nil
			}
			hm.mu.Lock()
			hm.nodeLastSeen[heartbeat.NodeID] = heartbeat.TimeStamp
			hm.nodeUsers[heartbeat.NodeID] = heartbeat.UserID
			hm.mu.Unlock()
		}
		return nil
	})
	if err != nil {
		return err
	}
	log.Printf("[%s] node state rebuilding complate : %d total heartbeat", hm.nodeID, totalProcessed)
	hm.mu.RLock()
	for nodeID, LastSeen := range hm.nodeLastSeen {
		users := hm.nodeUsers[nodeID]
		log.Printf("[%s] know node : %s (last need: %d, user: %d)", hm.nodeID, nodeID, LastSeen, len(users))
	}
	hm.mu.RUnlock()
	return nil
}

func (hm *HeartBeatManager) sendheatbeat() {
	ticker := time.NewTicker(hm.heartbeatIntervel)
	defer ticker.Stop()

	log.Printf("starting heartbeat sender for node %s", hm.nodeID)

	for {
		select {
		case <-hm.ctx.Done():
			log.Println("Heartbeat sender stopped")
			return
		case <-ticker.C:
			if err := hm.publishHeartbeat(); err != nil {
				log.Printf("faild to publish heartbeat: %s", &err)
			}
		}
	}
}

func (hm *HeartBeatManager) publishHeartbeat() error {
	usersID := hm.hub.GetConnectedUserID()
	heartbeat := NodeHeartBeat{
		NodeID:    hm.nodeID,
		TimeStamp: time.Now().Unix(),
		UserID:    usersID,
	}

	data, err := json.Marshal(heartbeat)
	if err != nil {
		return err
	}
	event := Event{
		Type: "node_heartbeat",
		Data: json.RawMessage(data),
	}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := kafka.Message{
		Key:   []byte(hm.nodeID),
		Value: eventBytes,
	}

	if err := hm.kafkaWriter.WriteMessages(ctx, msg); err != nil {
		return err
	}
	log.Printf("Heartbeat sent node=%s, user=%d,", hm.nodeID, len(usersID))
	return nil
}

func (hm *HeartBeatManager) listenToHeartBeat() {
	log.Println("heartbeat listener startes")

	for {
		select {
		case <-hm.ctx.Done():
			log.Println("heartbeat listener stopped")
			return
		default:
		}
		msg, err := hm.heartbeartReader.ReadMessage(hm.ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			log.Printf("kafka heartbeat read error : %v, retring in 1s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error Unmarshalling heartbeat event:%v", err)
			continue
		}
		if event.Type == "node_heartbeat" {
			var heartbeat NodeHeartBeat
			if err := json.Unmarshal(event.Data, &heartbeat); err != nil {
				log.Printf("Error Unmarshalling heartbeat :%v", err)
				continue
			}
			hm.handleHeartBeat(heartbeat)
		}
	}
}

func (hm *HeartBeatManager) handleHeartBeat(heartbeat NodeHeartBeat) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.nodeLastSeen[heartbeat.NodeID] = heartbeat.TimeStamp
	hm.nodeUsers[heartbeat.NodeID] = heartbeat.UserID

	if hm.processedDeadNodes[heartbeat.NodeID] {
		log.Printf("node %s is alive again! removing from dead list", heartbeat.NodeID)
		delete(hm.processedDeadNodes, heartbeat.NodeID)
	}

	log.Printf("Heartbeat received : node=%s, user=%d, timestamp=%d", heartbeat.NodeID, len(heartbeat.NodeID), heartbeat.TimeStamp)
}

func (hm *HeartBeatManager) monitorNodeHealth() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	log.Printf("node health monitor started")
	for {
		select {
		case <-hm.ctx.Done():
			log.Printf("node health monitor stopped")
			return
		case <-ticker.C:
			hm.checkDeadNode()
		}
	}
}
func (hm *HeartBeatManager) checkDeadNode() {
	now := time.Now().Unix()
	timeoutSeconds := int64(hm.timeoutThershould.Seconds())

	hm.mu.Lock()
	deadNodes := []string{}
	for nodeID, lastSeen := range hm.nodeLastSeen {
		if nodeID == hm.nodeID {
			continue
		}
		timeSinceLastSeen := now - lastSeen
		if timeSinceLastSeen > timeoutSeconds {
			if !hm.processedDeadNodes[nodeID] {
				deadNodes = append(deadNodes, nodeID)
				log.Printf("node %s is Dead (last seen %d seconds ago)", nodeID, timeSinceLastSeen)
			}
		}
	}
	hm.mu.Unlock()
	if len(deadNodes) > 0 {
		if hm.shouldHandleFailure() {
			for _, nodeID := range deadNodes {
				hm.HandleDeadNode(nodeID)
			}
		} else {
			log.Printf("not handing dead nodes %v - andther node is responsible", deadNodes)
		}
	}
}

func (hm *HeartBeatManager) HandleDeadNode(nodeID string) {
	hm.mu.Lock()
	usersID := hm.nodeUsers[nodeID]
	hm.processedDeadNodes[nodeID] = true
	hm.mu.Unlock()

	if len(usersID) == 0 {
		log.Printf("Dead node %s had no user", nodeID)
		return
	}
	log.Printf("Headling Dead Node %s : Marking %d user as offline", nodeID, len(usersID))

	for _, userID := range usersID {
		if err := hm.hub.PublishUserStatus(userID, nodeID, false); err != nil {
			log.Printf("fail to publish offline status for user %s:%v", userID, err)
		} else {

			log.Printf("User %s marked offline (from dead node : %s)", userID, nodeID)
		}
	}
}

func (hm *HeartBeatManager) shouldHandleFailure() bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	aliveNodes := []string{hm.nodeID}
	now := time.Now().Unix()
	timesoutSeconds := int64(hm.timeoutThershould.Seconds())

	for nodeID, lastSeen := range hm.nodeLastSeen {
		if nodeID == hm.nodeID {
			continue
		}
		timeSinceLastSeen := now - lastSeen
		if timeSinceLastSeen <= timesoutSeconds {
			aliveNodes = append(aliveNodes, nodeID)
		}
	}
	sort.Strings(aliveNodes)
	if len(aliveNodes) == 0 {
		return true
	}
	designatedNode := aliveNodes[0]
	isDesignated := designatedNode == hm.nodeID
	if isDesignated {
		log.Printf("Thsi node (%s) is Designated to handle dead nodes (alive noded: %v)", hm.nodeID, aliveNodes)
	}
	return isDesignated
}

func (hm *HeartBeatManager) cleanupOldDeadNodes() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	log.Printf("Dead node cleanup service started")
	for {
		select {
		case <-hm.ctx.Done():
			log.Println("Dead node cleanup Servvice Stopped ")
			return
		case <-ticker.C:
			now := time.Now().Unix()
			cleanupThereshould := int64(5 * 60)
			hm.mu.Lock()

			nodesToCleanup := []string{}
			for nodeID, lastSeen := range hm.nodeLastSeen {
				if nodeID == hm.nodeID {
					continue
				}

				timeSinceLastSeen := now - lastSeen
				if timeSinceLastSeen > cleanupThereshould && hm.processedDeadNodes[nodeID] {
					nodesToCleanup = append(nodesToCleanup, nodeID)
				}
			}
			// remove old dead node from memory
			for _, nodeID := range nodesToCleanup {
				log.Printf("forgetting old dead node : %s (last seen %d) seconds ago", nodeID, now-hm.nodeLastSeen[nodeID])
				delete(hm.nodeLastSeen, nodeID)
				delete(hm.nodeUsers, nodeID)
				delete(hm.processedDeadNodes, nodeID)
			}
			hm.mu.Unlock()

			if len(nodesToCleanup) > 0 {
				log.Printf("clean up %d old dead nodes ", len(nodesToCleanup))
			}
		}
	}
}

// stop func for gracful shoultdown
func (hm *HeartBeatManager) Stop() {
	log.Printf("stoping heartbeat manager..")
	hm.cancel()

	if hm.kafkaWriter != nil {
		hm.kafkaWriter.Close()
	}
	if hm.heartbeartReader != nil {
		hm.heartbeartReader.Close()
	}
	log.Printf("heartbeat manager stopped")
}
