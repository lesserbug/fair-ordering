// // pkg/ofo/service.go
// package ofo

// import (
// 	"SpeedFair_simplify/pkg/network"
// 	"SpeedFair_simplify/pkg/types"
// 	"context"
// 	"sort"
// 	"sync"
// 	"time"
// )

// const (
// 	replicaOrdersChanSize = 512
// 	batchReadyChanSize    = 256
// 	LEADER_REPLICA_ID     = 0
// 	TX_POOL_CAPACITY      = 50000
// )

// type OFOService struct {
// 	ReplicaID   uint64
// 	isLeader    bool
// 	isMalicious bool
// 	network     network.NetworkInterface

// 	// --- State with fine-grained locks ---
// 	txPoolMu          sync.RWMutex
// 	txPool            map[[32]byte]time.Time
// 	txSubmissionTimes map[[32]byte]time.Time

// 	committedMu    sync.RWMutex
// 	committedTxIDs map[[32]byte]bool

// 	deferredMu        sync.RWMutex
// 	deferredProposals map[uint64]*types.LeaderProposal

// 	currentBlockHeight uint64

// 	latencyMu                sync.Mutex
// 	totalLatency             time.Duration
// 	finalizedCountForLatency int64

// 	// --- Leader-specific pipeline ---
// 	pipelineCtx           context.Context
// 	pipelineCancel        context.CancelFunc
// 	pipelineWg            sync.WaitGroup
// 	replicaOrdersChan     chan *types.ReplicaOrders
// 	batchReadyChan        chan []*types.ReplicaOrders
// 	leaderCompletionChan  chan [][32]byte
// 	lastProposedOrderSize int

// 	// --- Config ---
// 	replicaCount uint64
// 	fFaulty      uint64
// 	gamma        float64
// 	loMaxSize    int
// 	loInterval   time.Duration
// }

// func NewOFOService(id, n, f uint64, gamma float64, net network.NetworkInterface, loSize int, loIntervalMs int, malicious bool) *OFOService {
// 	s := &OFOService{
// 		ReplicaID:         id,
// 		isLeader:          id == LEADER_REPLICA_ID,
// 		isMalicious:       malicious,
// 		network:           net,
// 		replicaCount:      n,
// 		fFaulty:           f,
// 		gamma:             gamma,
// 		loMaxSize:         loSize,
// 		loInterval:        time.Duration(loIntervalMs) * time.Millisecond,
// 		txPool:            make(map[[32]byte]time.Time),
// 		txSubmissionTimes: make(map[[32]byte]time.Time),
// 		committedTxIDs:    make(map[[32]byte]bool),
// 		deferredProposals: make(map[uint64]*types.LeaderProposal),
// 	}
// 	if s.isLeader {
// 		s.pipelineCtx, s.pipelineCancel = context.WithCancel(context.Background())
// 		s.replicaOrdersChan = make(chan *types.ReplicaOrders, replicaOrdersChanSize)
// 		s.batchReadyChan = make(chan []*types.ReplicaOrders, batchReadyChanSize)
// 		s.leaderCompletionChan = make(chan [][32]byte, 100)
// 		s.pipelineWg.Add(2)
// 		go s.runCollectorStage()
// 		go s.runProposerStage()
// 	}
// 	net.Register(id, s.handleMessage)
// 	return s
// }

// func (s *OFOService) Network() network.NetworkInterface { return s.network }

// func (s *OFOService) Start(ctx context.Context) {
// 	ticker := time.NewTicker(s.loInterval)
// 	defer ticker.Stop()
// 	for {
// 		select {
// 		case <-ticker.C:
// 			s.generateAndSendOrders()
// 		case <-ctx.Done():
// 			return
// 		}
// 	}
// }

// func (s *OFOService) Stop() {
// 	if s.isLeader {
// 		s.pipelineCancel()
// 		s.pipelineWg.Wait()
// 		close(s.leaderCompletionChan)
// 	}
// }

// func (s *OFOService) handleMessage(msg network.Message) {
// 	switch payload := msg.Payload.(type) {
// 	case *types.Transaction:
// 		s.committedMu.RLock()
// 		isCommitted := s.committedTxIDs[payload.ID]
// 		s.committedMu.RUnlock()
// 		if !isCommitted {
// 			s.txPoolMu.Lock()
// 			if _, exists := s.txPool[payload.ID]; !exists && len(s.txPool) < TX_POOL_CAPACITY {
// 				s.txPool[payload.ID] = time.Now()
// 				s.txSubmissionTimes[payload.ID] = payload.SubmissionTime
// 			}
// 			s.txPoolMu.Unlock()
// 		}
// 	case *types.ReplicaOrders:
// 		if s.isLeader && s.replicaOrdersChan != nil {
// 			select {
// 			case s.replicaOrdersChan <- payload:
// 			case <-s.pipelineCtx.Done():
// 			}
// 		}
// 	case *types.LeaderProposal:
// 		go s.handleProposal(payload)
// 	}
// }

// func (s *OFOService) generateAndSendOrders() {
// 	// Part 1: New transactions
// 	s.txPoolMu.RLock()
// 	var newTxOrder *types.LocalOrder
// 	if len(s.txPool) > 0 {
// 		poolTxs := make([]types.PoolTx, 0, len(s.txPool))
// 		for id, t := range s.txPool {
// 			poolTxs = append(poolTxs, types.PoolTx{ID: id, Time: t})
// 		}
// 		s.txPoolMu.RUnlock()

// 		sort.Slice(poolTxs, func(i, j int) bool { return poolTxs[i].Time.Before(poolTxs[j].Time) })
// 		batchSize := len(poolTxs)
// 		if s.loMaxSize > 0 && batchSize > s.loMaxSize {
// 			batchSize = s.loMaxSize
// 		}
// 		batchTxHashes := make([][32]byte, batchSize)
// 		for i := 0; i < batchSize; i++ {
// 			batchTxHashes[i] = poolTxs[i].ID
// 		}
// 		newTxOrder = &types.LocalOrder{ReplicaID: s.ReplicaID, OrderedTxs: batchTxHashes}
// 	} else {
// 		s.txPoolMu.RUnlock()
// 	}

// 	// Part 2: Deferred transactions (UpdateOrder)
// 	s.deferredMu.RLock()
// 	var deferredOrders []*types.UpdateOrder
// 	if len(s.deferredProposals) > 0 {
// 		s.txPoolMu.RLock()
// 		for height, proposal := range s.deferredProposals {
// 			txsInProposal := make([]types.PoolTx, 0, len(proposal.Graph.Nodes))
// 			for txID := range proposal.Graph.Nodes {
// 				if receiveTime, ok := s.txPool[txID]; ok {
// 					txsInProposal = append(txsInProposal, types.PoolTx{ID: txID, Time: receiveTime})
// 				}
// 			}
// 			sort.Slice(txsInProposal, func(i, j int) bool {
// 				return txsInProposal[i].Time.Before(txsInProposal[j].Time)
// 			})

// 			orderedTxHashes := make([][32]byte, len(txsInProposal))
// 			for i, ptx := range txsInProposal {
// 				orderedTxHashes[i] = ptx.ID
// 			}

// 			if len(orderedTxHashes) > 0 {
// 				deferredOrders = append(deferredOrders, &types.UpdateOrder{
// 					BlockHeight: height, OrderedTxs: orderedTxHashes,
// 				})
// 			}
// 		}
// 		s.txPoolMu.RUnlock()
// 	}
// 	s.deferredMu.RUnlock()

// 	// Part 3: Send the message
// 	orders := &types.ReplicaOrders{
// 		ReplicaID:   s.ReplicaID,
// 		NewTxOrder:  newTxOrder,
// 		DeferredTxs: deferredOrders,
// 	}

// 	msg := network.Message{From: s.ReplicaID, Payload: orders}
// 	if s.isLeader {
// 		s.handleMessage(msg)
// 	} else {
// 		msg.Type = "ReplicaOrders"
// 		msg.To = LEADER_REPLICA_ID
// 		s.network.Send(msg)
// 	}
// }

// func (s *OFOService) runCollectorStage() {
// 	defer s.pipelineWg.Done()
// 	if s.batchReadyChan != nil {
// 		defer close(s.batchReadyChan)
// 	}

// 	requiredOrders := s.replicaCount - s.fFaulty
// 	var pendingOrders []*types.ReplicaOrders
// 	receivedFrom := make(map[uint64]bool)

// 	timer := time.NewTimer(s.loInterval * 2)
// 	if !timer.Stop() {
// 		<-timer.C
// 	}

// 	sendBatch := func() {
// 		if len(pendingOrders) > 0 {
// 			batch := make([]*types.ReplicaOrders, len(pendingOrders))
// 			copy(batch, pendingOrders)
// 			select {
// 			case s.batchReadyChan <- batch:
// 			case <-s.pipelineCtx.Done():
// 			}
// 		}
// 		pendingOrders = nil
// 		receivedFrom = make(map[uint64]bool)
// 	}

// 	for {
// 		select {
// 		case order, ok := <-s.replicaOrdersChan:
// 			if !ok {
// 				sendBatch()
// 				return
// 			}
// 			if len(pendingOrders) == 0 {
// 				timer.Reset(s.loInterval * 2)
// 			}
// 			if !receivedFrom[order.ReplicaID] {
// 				pendingOrders = append(pendingOrders, order)
// 				receivedFrom[order.ReplicaID] = true
// 			}
// 			if uint64(len(pendingOrders)) >= requiredOrders {
// 				if !timer.Stop() {
// 					select {
// 					case <-timer.C:
// 					default:
// 					}
// 				}
// 				sendBatch()
// 			}
// 		case <-timer.C:
// 			sendBatch()
// 		case <-s.pipelineCtx.Done():
// 			return
// 		}
// 	}
// }

// func (s *OFOService) runProposerStage() {
// 	defer s.pipelineWg.Done()
// 	for {
// 		select {
// 		case <-s.pipelineCtx.Done():
// 			return
// 		case batchOrders, ok := <-s.batchReadyChan:
// 			if !ok {
// 				return
// 			}
// 			if uint64(len(batchOrders)) < s.replicaCount-s.fFaulty {
// 				continue
// 			}

// 			s.deferredMu.Lock()
// 			s.currentBlockHeight++
// 			currentHeight := s.currentBlockHeight
// 			deferredClone := make(map[uint64]*types.LeaderProposal)
// 			for h, p := range s.deferredProposals {
// 				deferredClone[h] = p
// 			}
// 			s.deferredMu.Unlock()

// 			s.committedMu.RLock()
// 			committedSnapshot := make(map[[32]byte]bool)
// 			for id := range s.committedTxIDs {
// 				committedSnapshot[id] = true
// 			}
// 			s.committedMu.RUnlock()

// 			newTxOrders, updateOrdersByHeight := s.separateOrderTypes(batchOrders)

// 			dm := NewDependencyManager()
// 			graphBuilt := dm.BuildGraphAndClassifyTxs(newTxOrders, s.replicaCount, s.fFaulty, s.gamma, committedSnapshot)

// 			if graphBuilt {
// 				dm.CutShadedTail()
// 			}

// 			updatesForProposal := make(map[uint64]map[[32]byte][][32]byte)
// 			for height, uos := range updateOrdersByHeight {
// 				if deferredProp, exists := deferredClone[height]; exists {
// 					newEdges := FairUpdate(uos, deferredProp.Graph, s.replicaCount, s.fFaulty, s.gamma)
// 					if len(newEdges) > 0 {
// 						updatesForProposal[height] = newEdges
// 					}
// 				}
// 			}

// 			s.lastProposedOrderSize = len(dm.graph.Nodes)
// 			proposal := &types.LeaderProposal{
// 				BlockHeight:    currentHeight,
// 				Graph:          dm.graph,
// 				TxStates:       dm.txStates,
// 				Updates:        updatesForProposal,
// 				Proof:          &types.LocalOrderFragment{ReplicaOrders: batchOrders},
// 				CommittedSoFar: committedSnapshot,
// 			}

// 			for i := uint64(0); i < s.replicaCount; i++ {
// 				msg := network.Message{From: s.ReplicaID, Payload: proposal}
// 				if i == s.ReplicaID {
// 					s.handleMessage(msg)
// 				} else {
// 					msg.To = i
// 					msg.Type = "LeaderProposal"
// 					s.network.Send(msg)
// 				}
// 			}
// 		}
// 	}
// }

// func (s *OFOService) separateOrderTypes(batchOrders []*types.ReplicaOrders) ([]*types.LocalOrder, map[uint64][]*types.UpdateOrder) {
// 	newTxOrders := make([]*types.LocalOrder, 0, len(batchOrders))
// 	updateOrdersByHeight := make(map[uint64][]*types.UpdateOrder)
// 	for _, ro := range batchOrders {
// 		if ro.NewTxOrder != nil && len(ro.NewTxOrder.OrderedTxs) > 0 {
// 			newTxOrders = append(newTxOrders, ro.NewTxOrder)
// 		}
// 		for _, uo := range ro.DeferredTxs {
// 			updateOrdersByHeight[uo.BlockHeight] = append(updateOrdersByHeight[uo.BlockHeight], uo)
// 		}
// 	}
// 	return newTxOrders, updateOrdersByHeight
// }

// func (s *OFOService) handleProposal(proposal *types.LeaderProposal) {
// 	if proposal == nil || proposal.Proof == nil || uint64(len(proposal.Proof.ReplicaOrders)) < s.replicaCount-s.fFaulty {
// 		return
// 	}

// 	s.deferredMu.Lock()
// 	if proposal.Updates != nil {
// 		for height, newEdges := range proposal.Updates {
// 			if deferredP, ok := s.deferredProposals[height]; ok {
// 				for u, vs := range newEdges {
// 					for _, v := range vs {
// 						addEdge(deferredP.Graph, u, v)
// 					}
// 				}
// 			}
// 		}
// 	}
// 	if proposal.Graph != nil && len(proposal.Graph.Nodes) > 0 {
// 		if _, exists := s.deferredProposals[proposal.BlockHeight]; !exists {
// 			s.deferredProposals[proposal.BlockHeight] = proposal
// 		}
// 	}

// 	// *** KEY FIX: Create a race-safe snapshot with deep-copied graphs ***
// 	deferredProposalsSnapshot := make(map[uint64]*types.LeaderProposal, len(s.deferredProposals))
// 	for h, p := range s.deferredProposals {
// 		if p == nil || p.Graph == nil {
// 			continue
// 		}
// 		// Create a lightweight deep copy of the graph
// 		ng := &types.DependencyGraph{
// 			Nodes: make(map[[32]byte]bool, len(p.Graph.Nodes)),
// 			Edges: make(map[[32]byte][][32]byte, len(p.Graph.Edges)),
// 		}
// 		for u := range p.Graph.Nodes {
// 			ng.Nodes[u] = true
// 		}
// 		for u, nbrs := range p.Graph.Edges {
// 			// Copy the slice to avoid sharing the underlying array
// 			cp := make([][32]byte, len(nbrs))
// 			copy(cp, nbrs)
// 			ng.Edges[u] = cp
// 		}

// 		// Create a shallow copy of the proposal struct, but replace the graph pointer
// 		clone := *p
// 		clone.Graph = ng
// 		deferredProposalsSnapshot[h] = &clone
// 	}
// 	s.deferredMu.Unlock()
// 	// *** END OF KEY FIX ***

// 	var allFinalizedTxs [][32]byte
// 	var heightsToFinalize []uint64

// 	sortedHeights := make([]uint64, 0, len(deferredProposalsSnapshot))
// 	for h := range deferredProposalsSnapshot {
// 		sortedHeights = append(sortedHeights, h)
// 	}
// 	sort.Slice(sortedHeights, func(i, j int) bool { return sortedHeights[i] < sortedHeights[j] })

// 	for _, height := range sortedHeights {
// 		deferredP := deferredProposalsSnapshot[height]
// 		dm := NewDependencyManager()
// 		dm.graph = deferredP.Graph
// 		dm.txStates = deferredP.TxStates

// 		if orderedSegment := dm.ComputeFairOrder(); len(orderedSegment) > 0 {
// 			allFinalizedTxs = append(allFinalizedTxs, orderedSegment...)
// 			heightsToFinalize = append(heightsToFinalize, height)
// 		}
// 	}

// 	if len(allFinalizedTxs) > 0 {
// 		s.commitFinalizedData(allFinalizedTxs, heightsToFinalize)
// 	}
// }

// func (s *OFOService) commitFinalizedData(finalizedTxs [][32]byte, heightsToFinalize []uint64) {
// 	if len(finalizedTxs) == 0 {
// 		return
// 	}
// 	finalizeTime := time.Now()

// 	s.deferredMu.Lock()
// 	for _, height := range heightsToFinalize {
// 		delete(s.deferredProposals, height)
// 	}
// 	s.deferredMu.Unlock()

// 	newlyFinalized := [][32]byte{}
// 	s.committedMu.Lock()
// 	for _, txID := range finalizedTxs {
// 		if !s.committedTxIDs[txID] {
// 			s.committedTxIDs[txID] = true
// 			newlyFinalized = append(newlyFinalized, txID)
// 		}
// 	}
// 	s.committedMu.Unlock()

// 	if len(newlyFinalized) == 0 {
// 		return
// 	}

// 	s.txPoolMu.Lock()
// 	s.latencyMu.Lock()
// 	for _, txID := range newlyFinalized {
// 		delete(s.txPool, txID)
// 		if submitTime, found := s.txSubmissionTimes[txID]; found {
// 			s.totalLatency += finalizeTime.Sub(submitTime)
// 			s.finalizedCountForLatency++
// 			delete(s.txSubmissionTimes, txID)
// 		}
// 	}
// 	s.latencyMu.Unlock()
// 	s.txPoolMu.Unlock()

// 	if s.isLeader && s.leaderCompletionChan != nil && len(newlyFinalized) > 0 {
// 		select {
// 		case s.leaderCompletionChan <- newlyFinalized:
// 		default:
// 		}
// 	}
// }

// // --- Getters with correct locking ---
// func (s *OFOService) GetLeaderCompletionChan() <-chan [][32]byte { return s.leaderCompletionChan }
// func (s *OFOService) GetFinalizedCount() int {
// 	s.committedMu.RLock()
// 	defer s.committedMu.RUnlock()
// 	return len(s.committedTxIDs)
// }
// func (s *OFOService) GetTxPoolSize() int {
// 	s.txPoolMu.RLock()
// 	defer s.txPoolMu.RUnlock()
// 	return len(s.txPool)
// }
// func (s *OFOService) GetLastProposedOrderSize() int { return s.lastProposedOrderSize }
// func (s *OFOService) GetLatencyStats() (avgLatency time.Duration, count int64) {
// 	s.latencyMu.Lock()
// 	defer s.latencyMu.Unlock()
// 	if s.finalizedCountForLatency == 0 {
// 		return 0, 0
// 	}
// 	return s.totalLatency / time.Duration(s.finalizedCountForLatency), s.finalizedCountForLatency
// }

package ofo

import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/types"
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Malicious behavior constants (start with slightly lower intensity for easier debugging)
const (
	maliciousDropFrac       = 0.15 // Start with 15%
	maliciousPairFlipFrac   = 0.60
	maliciousInjectInvalid  = 0.05 // Start with 5%
	maliciousIgnoreTxOnRecv = 0.20
)

const (
	replicaOrdersChanSize = 512
	batchReadyChanSize    = 256
	LEADER_REPLICA_ID     = 0
	TX_POOL_CAPACITY      = 50000
)

type OFOService struct {
	ReplicaID                uint64
	isLeader                 bool
	isMalicious              bool
	network                  network.NetworkInterface
	txPoolMu                 sync.RWMutex
	txPool                   map[[32]byte]time.Time
	txSubmissionTimes        map[[32]byte]time.Time
	committedMu              sync.RWMutex
	committedTxIDs           map[[32]byte]bool
	deferredMu               sync.RWMutex
	deferredProposals        map[uint64]*types.LeaderProposal
	currentBlockHeight       uint64
	latencyMu                sync.Mutex
	totalLatency             time.Duration
	finalizedCountForLatency int64
	pipelineCtx              context.Context
	pipelineCancel           context.CancelFunc
	pipelineWg               sync.WaitGroup
	replicaOrdersChan        chan *types.ReplicaOrders
	batchReadyChan           chan []*types.ReplicaOrders
	leaderCompletionChan     chan [][32]byte
	lastProposedOrderSize    int
	replicaCount             uint64
	fFaulty                  uint64
	gamma                    float64
	loMaxSize                int
	loInterval               time.Duration
}

func NewOFOService(id, n, f uint64, gamma float64, net network.NetworkInterface, loSize int, loIntervalMs int, malicious bool) *OFOService {
	s := &OFOService{
		ReplicaID:         id,
		isLeader:          id == LEADER_REPLICA_ID,
		isMalicious:       malicious,
		network:           net,
		replicaCount:      n,
		fFaulty:           f,
		gamma:             gamma,
		loMaxSize:         loSize,
		loInterval:        time.Duration(loIntervalMs) * time.Millisecond,
		txPool:            make(map[[32]byte]time.Time),
		txSubmissionTimes: make(map[[32]byte]time.Time),
		committedTxIDs:    make(map[[32]byte]bool),
		deferredProposals: make(map[uint64]*types.LeaderProposal),
	}
	if s.isLeader {
		s.pipelineCtx, s.pipelineCancel = context.WithCancel(context.Background())
		s.replicaOrdersChan = make(chan *types.ReplicaOrders, replicaOrdersChanSize)
		s.batchReadyChan = make(chan []*types.ReplicaOrders, batchReadyChanSize)
		s.leaderCompletionChan = make(chan [][32]byte, 100)
		s.pipelineWg.Add(2)
		go s.runCollectorStage()
		go s.runProposerStage()
	}
	net.Register(id, s.handleMessage)
	return s
}

// ... Network, Start, Stop, handleMessage methods are unchanged from your last version ...
// (We will only show modified functions below for clarity)
func (s *OFOService) Network() network.NetworkInterface {
	return s.network
}

func (s *OFOService) Start(ctx context.Context) {
	ticker := time.NewTicker(s.loInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.generateAndSendOrders()
		case <-ctx.Done():
			return
		}
	}
}

func (s *OFOService) Stop() {
	if s.isLeader {
		s.pipelineCancel()
		s.pipelineWg.Wait()
		if s.leaderCompletionChan != nil {
			close(s.leaderCompletionChan)
		}
	}
}

func (s *OFOService) handleMessage(msg network.Message) {
	switch payload := msg.Payload.(type) {
	case *types.Transaction:
		if s.isMalicious && rand.Float64() < maliciousIgnoreTxOnRecv {
			return
		}
		s.committedMu.RLock()
		isCommitted := s.committedTxIDs[payload.ID]
		s.committedMu.RUnlock()
		if !isCommitted {
			s.txPoolMu.Lock()
			if _, exists := s.txPool[payload.ID]; !exists && len(s.txPool) < TX_POOL_CAPACITY {
				s.txPool[payload.ID] = time.Now()
				s.txSubmissionTimes[payload.ID] = payload.SubmissionTime
			}
			s.txPoolMu.Unlock()
		}
	case *types.ReplicaOrders:
		if s.isLeader && s.replicaOrdersChan != nil {
			select {
			case s.replicaOrdersChan <- payload:
			case <-s.pipelineCtx.Done():
			}
		}
	case *types.LeaderProposal:
		go s.handleProposal(payload)
	}
}

func (s *OFOService) generateAndSendOrders() {
	// Part 1: New transactions (malicious logic remains)
	s.txPoolMu.RLock()
	var newTxOrder *types.LocalOrder
	if len(s.txPool) > 0 {
		poolTxs := make([]types.PoolTx, 0, len(s.txPool))
		for id, t := range s.txPool {
			poolTxs = append(poolTxs, types.PoolTx{ID: id, Time: t})
		}
		s.txPoolMu.RUnlock()

		sort.Slice(poolTxs, func(i, j int) bool {
			return poolTxs[i].Time.Before(poolTxs[j].Time)
		})

		if s.isMalicious {
			sort.Slice(poolTxs, func(i, j int) bool { return poolTxs[i].Time.After(poolTxs[j].Time) })
			for i := 0; i+1 < len(poolTxs); i += 2 {
				if rand.Float64() < maliciousPairFlipFrac {
					poolTxs[i], poolTxs[i+1] = poolTxs[i+1], poolTxs[i]
				}
			}
			kept := poolTxs[:0]
			for _, p := range poolTxs {
				if rand.Float64() > maliciousDropFrac {
					kept = append(kept, p)
				}
			}
			poolTxs = kept
		}

		batchSize := len(poolTxs)
		if s.loMaxSize > 0 && batchSize > s.loMaxSize {
			batchSize = s.loMaxSize
		}
		batchTxHashes := make([][32]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			batchTxHashes[i] = poolTxs[i].ID
		}

		if s.isMalicious {
			for i := 0; i < batchSize; i++ {
				if rand.Float64() < maliciousInjectInvalid {
					var fake [32]byte
					rand.Read(fake[:])
					batchTxHashes[i] = fake
				}
			}
		}
		newTxOrder = &types.LocalOrder{ReplicaID: s.ReplicaID, OrderedTxs: batchTxHashes}
	} else {
		s.txPoolMu.RUnlock()
	}

	// Part 2: Deferred transactions (malicious logic remains)
	s.deferredMu.RLock()
	var deferredOrders []*types.UpdateOrder
	if len(s.deferredProposals) > 0 {
		s.txPoolMu.RLock()
		for height, proposal := range s.deferredProposals {
			txsInProposal := make([]types.PoolTx, 0, len(proposal.Graph.Nodes))
			for txID := range proposal.Graph.Nodes {
				if receiveTime, ok := s.txPool[txID]; ok {
					txsInProposal = append(txsInProposal, types.PoolTx{ID: txID, Time: receiveTime})
				}
			}
			sort.Slice(txsInProposal, func(i, j int) bool {
				return txsInProposal[i].Time.Before(txsInProposal[j].Time)
			})

			orderedTxHashes := make([][32]byte, len(txsInProposal))
			for i, ptx := range txsInProposal {
				orderedTxHashes[i] = ptx.ID
			}

			if s.isMalicious {
				for l, r := 0, len(orderedTxHashes)-1; l < r; l, r = l+1, r-1 {
					orderedTxHashes[l], orderedTxHashes[r] = orderedTxHashes[r], orderedTxHashes[l]
				}
			}

			if len(orderedTxHashes) > 0 {
				deferredOrders = append(deferredOrders, &types.UpdateOrder{
					BlockHeight: height,
					OrderedTxs:  orderedTxHashes,
				})
			}
		}
		s.txPoolMu.RUnlock()
	}
	s.deferredMu.RUnlock()

	// Part 3: Send message (remains the same)
	orders := &types.ReplicaOrders{
		ReplicaID:   s.ReplicaID,
		NewTxOrder:  newTxOrder,
		DeferredTxs: deferredOrders,
	}
	msg := network.Message{From: s.ReplicaID, Payload: orders}
	if s.isLeader {
		s.handleMessage(msg)
	} else {
		msg.Type = "ReplicaOrders"
		msg.To = LEADER_REPLICA_ID
		s.network.Send(msg)
	}
}

// <<< FIX: Validation is now more robust against network timing races >>>
func (s *OFOService) validateReplicaOrders(ro *types.ReplicaOrders) bool {
	if ro.NewTxOrder != nil {
		if s.loMaxSize > 0 && len(ro.NewTxOrder.OrderedTxs) > s.loMaxSize {
			return false
		}
		seen := make(map[[32]byte]bool)
		for _, id := range ro.NewTxOrder.OrderedTxs {
			if seen[id] {
				return false
			}
			seen[id] = true

			// Relaxed check: As long as the leader has seen the tx in either its
			// submission map or its current pool, it's valid. This avoids penalizing
			// honest nodes due to network race conditions.
			s.txPoolMu.RLock()
			_, ok1 := s.txSubmissionTimes[id]
			_, ok2 := s.txPool[id]
			s.txPoolMu.RUnlock()
			if !(ok1 || ok2) {
				return false // A truly unknown (likely fake) transaction
			}
		}
	}

	for _, u := range ro.DeferredTxs {
		s.deferredMu.RLock()
		p, p_ok := s.deferredProposals[u.BlockHeight]
		s.deferredMu.RUnlock()
		if !p_ok || p == nil || p.Graph == nil {
			return false
		}
		for _, id := range u.OrderedTxs {
			if _, ok := p.Graph.Nodes[id]; !ok {
				return false
			}
		}
	}

	return true
}

// ... runCollectorStage is unchanged, its logic is correct ...
func (s *OFOService) runCollectorStage() {
	defer s.pipelineWg.Done()
	if s.batchReadyChan != nil {
		defer close(s.batchReadyChan)
	}
	requiredOrders := s.replicaCount - s.fFaulty
	var pendingOrders []*types.ReplicaOrders
	receivedFrom := make(map[uint64]bool)
	timer := time.NewTimer(s.loInterval * 2)
	if !timer.Stop() {
		<-timer.C
	}

	sendBatch := func() {
		if len(pendingOrders) > 0 {
			batch := make([]*types.ReplicaOrders, len(pendingOrders))
			copy(batch, pendingOrders)
			select {
			case s.batchReadyChan <- batch:
			case <-s.pipelineCtx.Done():
			}
		}
		pendingOrders = nil
		receivedFrom = make(map[uint64]bool)
	}

	for {
		select {
		case order, ok := <-s.replicaOrdersChan:
			if !ok {
				sendBatch()
				return
			}

			if len(pendingOrders) == 0 {
				timer.Reset(s.loInterval * 2)
			}

			if !s.validateReplicaOrders(order) {
				continue
			}

			if !receivedFrom[order.ReplicaID] {
				pendingOrders = append(pendingOrders, order)
				receivedFrom[order.ReplicaID] = true
			}

			if uint64(len(pendingOrders)) >= requiredOrders {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				sendBatch()
			}
		case <-timer.C:
			sendBatch()
		case <-s.pipelineCtx.Done():
			return
		}
	}
}

func (s *OFOService) runProposerStage() {
	defer s.pipelineWg.Done()
	for {
		select {
		case <-s.pipelineCtx.Done():
			return
		case batchOrders, ok := <-s.batchReadyChan:
			if !ok {
				return
			}

			// <<< CRITICAL FIX A: Restore the n-f threshold for making proposals >>>
			// The proposer MUST NOT create a proposal unless it has sufficient evidence.
			// Batches triggered by the collector's timer will often be smaller than this
			// and must be ignored to prevent proposing empty graphs.
			if uint64(len(batchOrders)) < s.replicaCount-s.fFaulty {
				continue
			}

			s.deferredMu.Lock()
			s.currentBlockHeight++
			currentHeight := s.currentBlockHeight
			deferredClone := make(map[uint64]*types.LeaderProposal)
			for h, p := range s.deferredProposals {
				deferredClone[h] = p
			}
			s.deferredMu.Unlock()
			s.committedMu.RLock()
			committedSnapshot := make(map[[32]byte]bool)
			for id := range s.committedTxIDs {
				committedSnapshot[id] = true
			}
			s.committedMu.RUnlock()
			newTxOrders, updateOrdersByHeight := s.separateOrderTypes(batchOrders)
			dm := NewDependencyManager()
			graphBuilt := dm.BuildGraphAndClassifyTxs(newTxOrders, s.replicaCount, s.fFaulty, s.gamma, committedSnapshot)

			// Only proceed if a non-empty graph was built
			if graphBuilt {
				dm.CutShadedTail()
			}

			updatesForProposal := make(map[uint64]map[[32]byte][][32]byte)
			for height, uos := range updateOrdersByHeight {
				if deferredProp, exists := deferredClone[height]; exists {
					newEdges := FairUpdate(uos, deferredProp.Graph, s.replicaCount, s.fFaulty, s.gamma)
					if len(newEdges) > 0 {
						updatesForProposal[height] = newEdges
					}
				}
			}
			s.lastProposedOrderSize = len(dm.graph.Nodes)
			proposal := &types.LeaderProposal{
				BlockHeight:    currentHeight,
				Graph:          dm.graph,
				TxStates:       dm.txStates,
				Updates:        updatesForProposal,
				Proof:          &types.LocalOrderFragment{ReplicaOrders: batchOrders},
				CommittedSoFar: committedSnapshot,
			}
			for i := uint64(0); i < s.replicaCount; i++ {
				msg := network.Message{From: s.ReplicaID, Payload: proposal}
				if i == s.ReplicaID {
					s.handleMessage(msg)
				} else {
					msg.To = i
					msg.Type = "LeaderProposal"
					s.network.Send(msg)
				}
			}
		}
	}
}

func (s *OFOService) handleProposal(proposal *types.LeaderProposal) {
	// <<< CRITICAL FIX B: Restore the n-f threshold for accepting proposals >>>
	// A replica must only accept proposals that are backed by sufficient proof.
	if proposal == nil || proposal.Proof == nil ||
		uint64(len(proposal.Proof.ReplicaOrders)) < s.replicaCount-s.fFaulty {
		return
	}

	s.deferredMu.Lock()
	if proposal.Updates != nil {
		for height, newEdges := range proposal.Updates {
			if deferredP, ok := s.deferredProposals[height]; ok {
				for u, vs := range newEdges {
					for _, v := range vs {
						addEdge(deferredP.Graph, u, v)
					}
				}
			}
		}
	}
	// Only add non-empty graphs to deferred proposals
	if proposal.Graph != nil && len(proposal.Graph.Nodes) > 0 {
		if _, exists := s.deferredProposals[proposal.BlockHeight]; !exists {
			s.deferredProposals[proposal.BlockHeight] = proposal
		}
	}

	// The deep copy logic for snapshotting is correct and important, leave as is.
	deferredProposalsSnapshot := make(map[uint64]*types.LeaderProposal, len(s.deferredProposals))
	for h, p := range s.deferredProposals {
		if p == nil || p.Graph == nil {
			continue
		}
		ng := &types.DependencyGraph{
			Nodes: make(map[[32]byte]bool, len(p.Graph.Nodes)),
			Edges: make(map[[32]byte][][32]byte, len(p.Graph.Edges)),
		}
		for u := range p.Graph.Nodes {
			ng.Nodes[u] = true
		}
		for u, nbrs := range p.Graph.Edges {
			cp := make([][32]byte, len(nbrs))
			copy(cp, nbrs)
			ng.Edges[u] = cp
		}
		clone := *p
		clone.Graph = ng
		deferredProposalsSnapshot[h] = &clone
	}
	s.deferredMu.Unlock()

	// The finalization logic remains correct.
	var allFinalizedTxs [][32]byte
	var heightsToFinalize []uint64
	sortedHeights := make([]uint64, 0, len(deferredProposalsSnapshot))
	for h := range deferredProposalsSnapshot {
		sortedHeights = append(sortedHeights, h)
	}
	sort.Slice(sortedHeights, func(i, j int) bool { return sortedHeights[i] < sortedHeights[j] })

	for _, height := range sortedHeights {
		deferredP := deferredProposalsSnapshot[height]
		if deferredP.Graph == nil || len(deferredP.Graph.Nodes) == 0 {
			continue
		}
		dm := NewDependencyManager()
		dm.graph = deferredP.Graph
		dm.txStates = deferredP.TxStates
		if orderedSegment := dm.ComputeFairOrder(); len(orderedSegment) > 0 {
			allFinalizedTxs = append(allFinalizedTxs, orderedSegment...)
			heightsToFinalize = append(heightsToFinalize, height)
		}
	}
	if len(allFinalizedTxs) > 0 {
		s.commitFinalizedData(allFinalizedTxs, heightsToFinalize)
	}
}

// ... commitFinalizedData and all Getter methods are unchanged ...
// They can be copied directly from your last working version.
func (s *OFOService) separateOrderTypes(batchOrders []*types.ReplicaOrders) ([]*types.LocalOrder, map[uint64][]*types.UpdateOrder) {
	newTxOrders := make([]*types.LocalOrder, 0, len(batchOrders))
	updateOrdersByHeight := make(map[uint64][]*types.UpdateOrder)
	for _, ro := range batchOrders {
		if ro.NewTxOrder != nil && len(ro.NewTxOrder.OrderedTxs) > 0 {
			newTxOrders = append(newTxOrders, ro.NewTxOrder)
		}
		for _, uo := range ro.DeferredTxs {
			updateOrdersByHeight[uo.BlockHeight] = append(updateOrdersByHeight[uo.BlockHeight], uo)
		}
	}
	return newTxOrders, updateOrdersByHeight
}

func (s *OFOService) commitFinalizedData(finalizedTxs [][32]byte, heightsToFinalize []uint64) {
	if len(finalizedTxs) == 0 {
		return
	}
	finalizeTime := time.Now()
	s.deferredMu.Lock()
	for _, height := range heightsToFinalize {
		delete(s.deferredProposals, height)
	}
	s.deferredMu.Unlock()
	newlyFinalized := [][32]byte{}
	s.committedMu.Lock()
	for _, txID := range finalizedTxs {
		if !s.committedTxIDs[txID] {
			s.committedTxIDs[txID] = true
			newlyFinalized = append(newlyFinalized, txID)
		}
	}
	s.committedMu.Unlock()
	if len(newlyFinalized) == 0 {
		return
	}
	s.txPoolMu.Lock()
	s.latencyMu.Lock()
	for _, txID := range newlyFinalized {
		delete(s.txPool, txID)
		if submitTime, found := s.txSubmissionTimes[txID]; found {
			s.totalLatency += finalizeTime.Sub(submitTime)
			s.finalizedCountForLatency++
			delete(s.txSubmissionTimes, txID)
		}
	}
	s.latencyMu.Unlock()
	s.txPoolMu.Unlock()
	if s.isLeader && s.leaderCompletionChan != nil && len(newlyFinalized) > 0 {
		select {
		case s.leaderCompletionChan <- newlyFinalized:
		default:
		}
	}
}
func (s *OFOService) GetLeaderCompletionChan() <-chan [][32]byte {
	return s.leaderCompletionChan
}
func (s *OFOService) GetFinalizedCount() int {
	s.committedMu.RLock()
	defer s.committedMu.RUnlock()
	return len(s.committedTxIDs)
}
func (s *OFOService) GetTxPoolSize() int {
	s.txPoolMu.RLock()
	defer s.txPoolMu.RUnlock()
	return len(s.txPool)
}
func (s *OFOService) GetLastProposedOrderSize() int {
	return s.lastProposedOrderSize
}
func (s *OFOService) GetLatencyStats() (avgLatency time.Duration, count int64) {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()
	if s.finalizedCountForLatency == 0 {
		return 0, 0
	}
	return s.totalLatency / time.Duration(s.finalizedCountForLatency), s.finalizedCountForLatency
}
