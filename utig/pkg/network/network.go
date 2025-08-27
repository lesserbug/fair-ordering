// pkg/network/network.go
package network

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type NetworkInterface interface {
	Register(replicaID uint64, handler func(Message))
	Send(msg Message)
	Stop()
	WaitForPeers(timeout time.Duration) error // <<< ADDED: 接口
}

type Message struct {
	Type    string
	From    uint64
	To      uint64
	Payload interface{}
}

type NetworkConfig struct {
	ReplicaID   uint64
	ReplicaAddr map[uint64]string
}

type DistributedNetwork struct {
	config       NetworkConfig
	handler      func(Message)
	connections  map[uint64]*peerConn
	listener     net.Listener
	mu           sync.RWMutex
	stopChan     chan struct{}
	wg           sync.WaitGroup
	msgQueue     chan Message
	workerCount  int
	connAttempts map[uint64]bool
	connMu       sync.Mutex

	readyChan chan struct{} // <<< ADDED
	readyOnce sync.Once     // <<< ADDED
}

type peerConn struct {
	conn net.Conn
	enc  *gob.Encoder
	mu   sync.Mutex
}

func NewDistributedNetwork(config NetworkConfig) (*DistributedNetwork, error) {
	n := &DistributedNetwork{
		config:       config,
		connections:  make(map[uint64]*peerConn),
		stopChan:     make(chan struct{}),
		msgQueue:     make(chan Message, 10000),
		workerCount:  16,
		connAttempts: make(map[uint64]bool),
		readyChan:    make(chan struct{}), // <<< ADDED
	}
	addr := config.ReplicaAddr[config.ReplicaID]
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("node %d failed to listen on %s: %v", config.ReplicaID, addr, err)
	}
	n.listener = listener
	for i := 0; i < n.workerCount; i++ {
		n.wg.Add(1)
		go n.messageWorker()
	}
	n.wg.Add(1)
	go n.acceptConnections()
	go n.connectionManager()
	return n, nil
}

func (n *DistributedNetwork) messageWorker() {
	defer n.wg.Done()
	for {
		select {
		case msg := <-n.msgQueue:
			if n.handler != nil {
				n.handler(msg)
			}
		case <-n.stopChan:
			return
		}
	}
}

func (n *DistributedNetwork) acceptConnections() {
	defer n.wg.Done()
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.stopChan:
				return
			default:
				continue
			}
		}
		go n.handleIncomingConnection(conn)
	}
}

func (n *DistributedNetwork) handleIncomingConnection(conn net.Conn) {
	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)
	var peerID uint64
	if err := decoder.Decode(&peerID); err != nil {
		conn.Close()
		return
	}
	n.mu.Lock()
	if existingConn, exists := n.connections[peerID]; exists {
		existingConn.conn.Close()
	}
	n.connections[peerID] = &peerConn{conn: conn, enc: encoder}
	n.mu.Unlock()
	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			n.mu.Lock()
			if c, ok := n.connections[peerID]; ok && c.conn == conn {
				delete(n.connections, peerID)
			}
			n.mu.Unlock()
			conn.Close()
			return
		}
		select {
		case n.msgQueue <- msg:
		case <-n.stopChan:
			return
		}
	}
}

func (n *DistributedNetwork) connectionManager() {
	time.AfterFunc(2*time.Second, func() {
		for replicaID, addr := range n.config.ReplicaAddr {
			if replicaID != n.config.ReplicaID {
				go n.tryConnect(replicaID, addr)
			}
		}
	})

	ticker := time.NewTicker(2 * time.Second) // 缩短检查间隔
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			totalPeers := len(n.config.ReplicaAddr) - 1
			if totalPeers <= 0 { // 如果只有一个节点，直接ready
				n.readyOnce.Do(func() { close(n.readyChan) })
				return
			}

			n.mu.RLock()
			connectedCount := len(n.connections)
			n.mu.RUnlock()

			// <<< 检查是否所有节点都已连接 >>>
			if connectedCount >= totalPeers {
				n.readyOnce.Do(func() { close(n.readyChan) })
			} else {
				// 如果未全部连接，尝试连接尚未连接的节点
				for replicaID, addr := range n.config.ReplicaAddr {
					if replicaID == n.config.ReplicaID {
						continue
					}
					n.mu.RLock()
					_, exists := n.connections[replicaID]
					n.mu.RUnlock()
					if !exists {
						go n.tryConnect(replicaID, addr)
					}
				}
			}

		case <-n.stopChan:
			return
		}
	}
}

func (n *DistributedNetwork) WaitForPeers(timeout time.Duration) error {
	select {
	case <-n.readyChan:
		log.Printf("Node %d: All peers connected.", n.config.ReplicaID)
		return nil
	case <-time.After(timeout):
		n.mu.RLock()
		defer n.mu.RUnlock()
		return fmt.Errorf("node %d timed out waiting for peers. Connected to %d/%d", n.config.ReplicaID, len(n.connections), len(n.config.ReplicaAddr)-1)
	}
}

func (n *DistributedNetwork) tryConnect(replicaID uint64, addr string) {
	n.connMu.Lock()
	if n.connAttempts[replicaID] {
		n.connMu.Unlock()
		return
	}
	n.connAttempts[replicaID] = true
	n.connMu.Unlock()
	defer func() { n.connMu.Lock(); n.connAttempts[replicaID] = false; n.connMu.Unlock() }()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return
	}
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	if err = encoder.Encode(n.config.ReplicaID); err != nil {
		conn.Close()
		return
	}
	n.mu.Lock()
	if existingConn, exists := n.connections[replicaID]; exists {
		existingConn.conn.Close()
	}
	n.connections[replicaID] = &peerConn{conn: conn, enc: encoder}
	n.mu.Unlock()
	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			n.mu.Lock()
			if c, ok := n.connections[replicaID]; ok && c.conn == conn {
				delete(n.connections, replicaID)
			}
			n.mu.Unlock()
			conn.Close()
			return
		}
		select {
		case n.msgQueue <- msg:
		case <-n.stopChan:
			return
		}
	}
}

func (n *DistributedNetwork) Register(replicaID uint64, handler func(Message)) {
	if replicaID == n.config.ReplicaID {
		n.handler = handler
	}
}

func (n *DistributedNetwork) Send(msg Message) {
	if msg.To == n.config.ReplicaID {
		select {
		case n.msgQueue <- msg:
		default:
		}
		return
	}
	n.mu.RLock()
	pc, ok := n.connections[msg.To]
	n.mu.RUnlock()
	if !ok {
		return
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if err := pc.enc.Encode(msg); err != nil {
		n.mu.Lock()
		if c, ok := n.connections[msg.To]; ok && c == pc {
			delete(n.connections, msg.To)
		}
		n.mu.Unlock()
		pc.conn.Close()
	}
}

func (n *DistributedNetwork) Stop() {
	close(n.stopChan)
	if n.listener != nil {
		n.listener.Close()
	}
	n.mu.Lock()
	for _, pc := range n.connections {
		pc.conn.Close()
	}
	n.mu.Unlock()
	n.wg.Wait()
}
