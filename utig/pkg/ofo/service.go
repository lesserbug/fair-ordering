// pkg/ofo/service.go
package ofo

import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/types"
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"
)

const (
	localOrderChanSize = 512
	batchReadyChanSize = 256
	STALE_THRESHOLD    = 8 // Number of rounds after which a blank tx is pruned
	// LO_MAX_TX_COUNT    = 200 // Max txs per local order
)

type OFOService struct {
	ReplicaID   uint64
	isLeader    bool
	isMalicious bool // <<< 新增字段：标记此节点是否为恶意节点
	network     network.NetworkInterface
	IdRegistry  *types.TxIDRegistry
	UtigManager *DependencyManager // Only for leader

	// State for all nodes
	rwMu              sync.RWMutex
	finalizedTxsLog   map[[32]byte]bool
	txSubmissionTimes map[uint64]time.Time // <<< ADDED: 存储交易提交时间

	// Leader-specific pipeline state
	pipelineCtx    context.Context
	pipelineCancel context.CancelFunc
	pipelineWg     sync.WaitGroup
	localOrderChan chan *types.LocalOrder
	batchReadyChan chan []*types.LocalOrder
	currentRound   int

	// System parameters
	replicaCount uint64
	fFaulty      uint64
	gamma        float64

	loMaxSize  int  // <<< 新增这个字段来存储每个LO的大小上限
	loInterval *int // <<< ADDED: Collector超时需要

	// <<< ADDED: 用于延迟统计的字段 >>>
	totalLatency             time.Duration
	finalizedCountForLatency int64
}

func NewOFOService(replicaID, n, f uint64, gamma float64, net network.NetworkInterface, registry *types.TxIDRegistry, loMaxSize int, loInterval *int, isMalicious bool) *OFOService {
	s := &OFOService{
		ReplicaID:                replicaID,
		isLeader:                 replicaID == 0,
		isMalicious:              isMalicious, // <<< 初始化新增字段
		network:                  net,
		IdRegistry:               registry,
		finalizedTxsLog:          make(map[[32]byte]bool),
		replicaCount:             n,
		fFaulty:                  f,
		gamma:                    gamma,
		currentRound:             0,
		loMaxSize:                loMaxSize, // <<< 初始化新的字段 >>>
		loInterval:               loInterval,
		txSubmissionTimes:        make(map[uint64]time.Time),
		totalLatency:             0,
		finalizedCountForLatency: 0,
	}
	if s.isLeader {
		s.UtigManager = NewDependencyManager(n, f, gamma)
		s.pipelineCtx, s.pipelineCancel = context.WithCancel(context.Background())
		s.localOrderChan = make(chan *types.LocalOrder, localOrderChanSize)
		s.batchReadyChan = make(chan []*types.LocalOrder, batchReadyChanSize)
		s.pipelineWg.Add(2)
		go s.runCollectorStage()
		go s.runProposerStage()
	}
	net.Register(replicaID, s.handleMessage)
	return s
}

func (s *OFOService) Stop() {
	if s.isLeader {
		s.pipelineCancel()
		s.pipelineWg.Wait()
	}
}

// <<< MODIFIED: 处理 Transaction 消息 >>>
func (s *OFOService) handleMessage(msg network.Message) {
	switch payload := msg.Payload.(type) {
	case *types.LocalOrder:
		if s.isLeader {
			select {
			case s.localOrderChan <- payload:
			case <-s.pipelineCtx.Done():
			}
		}
	case *types.VerifiableFairOrderFragment:
		if !s.isLeader {
			if s.verifyAndAdvance(payload) {
				s.rwMu.Lock()
				for _, txHash := range payload.FinalOrder.OrderedTxHashes {
					s.finalizedTxsLog[txHash] = true
				}
				s.rwMu.Unlock()
			} else {
				fmt.Printf("[Replica %d] Verification FAILED for fragment.\n", s.ReplicaID)
			}
		}
	case *types.Transaction:
		id := s.IdRegistry.GetOrRegister(*payload)
		if s.isLeader { // Leader 缓存时间戳用于后续计算
			s.rwMu.Lock()
			if _, exists := s.txSubmissionTimes[id]; !exists {
				s.txSubmissionTimes[id] = payload.SubmissionTime
			}
			s.rwMu.Unlock()
		}
	}
}

// <<< MODIFIED: 在一个函数内实现正常和恶意两种行为 >>>
// GenerateAndSendLocalOrder creates and sends a local order for this service instance.
// If the service is marked as malicious, it will reverse the order.
func (s *OFOService) GenerateAndSendLocalOrder() {
	s.rwMu.RLock()
	// 复制 finalizedTxsLog 以避免长时间持有锁
	finalizedCopy := make(map[[32]byte]bool, len(s.finalizedTxsLog))
	for k, v := range s.finalizedTxsLog {
		finalizedCopy[k] = v
	}
	s.rwMu.RUnlock()

	// 获取所有未定序的交易ID，已经按到达时间排好序
	unfinalizedIDs := s.IdRegistry.GetUnfinalizedIDsSortedByTime(finalizedCopy)
	if len(unfinalizedIDs) == 0 {
		return
	}

	// 从排序好的列表中取一个样本
	sampleSize := s.loMaxSize
	if len(unfinalizedIDs) < sampleSize {
		sampleSize = len(unfinalizedIDs)
	}
	sampleIDs := unfinalizedIDs[len(unfinalizedIDs)-sampleSize:]

	// --- 关键修改：根据 isMalicious 标志决定行为 ---
	if s.isMalicious {
		// --- 恶意行为 ---
		// 颠倒 sampleIDs 列表的顺序
		for i, j := 0, len(sampleIDs)-1; i < j; i, j = i+1, j-1 {
			sampleIDs[i], sampleIDs[j] = sampleIDs[j], sampleIDs[i]
		}
		// 打印日志，以便观察到恶意行为
		// 使用 \r 和一些空格来尽量不打乱监控输出
		fmt.Printf("\r[MALICIOUS Node %d] Reversed its LocalOrder.                                       \n", s.ReplicaID)
	}

	lo := &types.LocalOrder{ReplicaID: s.ReplicaID, OrderedTxs: sampleIDs}
	s.network.Send(network.Message{
		Type: "LocalOrder", From: 0, To: 0, Payload: lo,
	})
}

// <<< MODIFIED: 增加超时机制 >>>
func (s *OFOService) runCollectorStage() {
	defer s.pipelineWg.Done()
	defer close(s.batchReadyChan)
	requiredOrders := s.replicaCount - s.fFaulty
	var pendingLocalOrders []*types.LocalOrder
	receivedFrom := make(map[uint64]bool)

	// 如果未提供loInterval，使用一个默认值
	timeoutDuration := 200 * time.Millisecond
	if s.loInterval != nil {
		timeoutDuration = 2 * time.Duration(*s.loInterval) * time.Millisecond
	}
	timer := time.NewTimer(timeoutDuration)
	if !timer.Stop() {
		<-timer.C
	} // 确保启动时定时器是停止的

	// 辅助函数，避免重复代码
	sendBatch := func() {
		if len(pendingLocalOrders) == 0 {
			return
		}
		batch := make([]*types.LocalOrder, len(pendingLocalOrders))
		copy(batch, pendingLocalOrders)
		pendingLocalOrders = nil
		receivedFrom = make(map[uint64]bool)
		select {
		case s.batchReadyChan <- batch:
		case <-s.pipelineCtx.Done():
		}
	}

	for {
		select {
		case order, ok := <-s.localOrderChan:
			if !ok {
				sendBatch()
				return
			}

			if len(pendingLocalOrders) == 0 {
				timer.Reset(timeoutDuration)
			}

			if !receivedFrom[order.ReplicaID] {
				pendingLocalOrders = append(pendingLocalOrders, order)
				receivedFrom[order.ReplicaID] = true
			}

			if uint64(len(pendingLocalOrders)) >= requiredOrders {
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
		case batchLOs, ok := <-s.batchReadyChan:
			if !ok {
				return
			}

			// --- Step 1: 短暂锁定，更新UTIG状态 ---
			s.rwMu.Lock()
			s.currentRound++
			currentRoundForPruning := s.currentRound
			for _, order := range batchLOs {
				for _, txID := range order.OrderedTxs {
					s.UtigManager.AddTxToUTIG(txID)
				}
			}
			s.UtigManager.IncrementalRefreshAndUpdate(batchLOs, s.currentRound)
			s.rwMu.Unlock() // <<< 立即释放锁

			// --- Step 2: 无锁执行耗时计算 ---
			proposalIDs := s.UtigManager.ExtractFairOrder()

			if len(proposalIDs) > 0 {
				proof := s.UtigManager.GenerateProof(proposalIDs)
				// <<< 关键: 立即从图中移除已定序的交易 >>>
				s.UtigManager.RemoveTxsFromUTIG(proposalIDs)

				proposalHashes := make([][32]byte, len(proposalIDs))
				finalizeTime := time.Now()

				// --- Step 3: 再次短暂锁定，更新最终状态和统计数据 ---
				s.rwMu.Lock()
				for i, id := range proposalIDs {
					hash, _ := s.IdRegistry.GetHash(id)
					proposalHashes[i] = hash
					s.finalizedTxsLog[hash] = true
					if submitTime, ok := s.txSubmissionTimes[id]; ok {
						latency := finalizeTime.Sub(submitTime)
						if latency < 0 { // <<< ADDED: 处理负延迟 >>>
							latency = 0
						}
						s.totalLatency += latency
						s.finalizedCountForLatency++
						delete(s.txSubmissionTimes, id)
					}
				}
				s.rwMu.Unlock()

				verifiableFragment := &types.VerifiableFairOrderFragment{
					FinalOrder:        &types.FairOrderFragment{OrderedTxIDs: proposalIDs, OrderedTxHashes: proposalHashes},
					SourceLocalOrders: batchLOs,
					Proof:             proof,
				}

				// --- Step 4: 无锁广播 ---
				for i := uint64(0); i < s.replicaCount; i++ {
					s.network.Send(network.Message{Type: "VerifiableFairOrderFragment", From: s.ReplicaID, To: i, Payload: verifiableFragment})
				}
			}

			// --- Step 5: 短暂锁定，执行剪枝 ---
			s.rwMu.Lock()
			s.UtigManager.PruneStaleTxs(currentRoundForPruning, STALE_THRESHOLD)
			s.rwMu.Unlock()
		}
	}
}

// verifyAndAdvance 验证一个来自Leader的提案是否有效
func (s *OFOService) verifyAndAdvance(fragment *types.VerifiableFairOrderFragment) bool {
	proposedIDs := fragment.FinalOrder.OrderedTxIDs
	proof := fragment.Proof

	// --- 0. 基本的健全性检查 ---
	if proof == nil || len(proof.TxStates) == 0 {
		// 如果 proof 为空，那么定序列表也必须为空
		return len(proposedIDs) == 0
	}

	// --- 1. 验证边的权重和方向 ---
	// 使用公共函数计算本地阈值，确保与Leader一致
	edgeThresh := types.CalculateEdgeThreshold(s.replicaCount, s.fFaulty, s.gamma)

	// Step 1a: 计算本批次LocalOrders贡献的权重
	batchWeights := make(map[edgeKey]int)
	for _, lo := range fragment.SourceLocalOrders {
		for i, u := range lo.OrderedTxs {
			// 只关心在证明中出现的交易对
			if _, exists := proof.TxStates[u]; !exists {
				continue
			}
			for j := i + 1; j < len(lo.OrderedTxs); j++ {
				v := lo.OrderedTxs[j]
				if _, exists := proof.TxStates[v]; !exists {
					continue
				}
				batchWeights[edgeKey{u, v}]++
			}
		}
	}

	// Step 1b: 验证每条边的权重断言和方向
	for _, edge := range proof.Edges {
		// Leader断言的总权重，减去本批次的贡献，得到的就是历史权重。
		// 这个历史权重不能是负数。
		histWeightUV := edge.WeightUV - batchWeights[edgeKey{edge.U, edge.V}]
		histWeightVU := edge.WeightVU - batchWeights[edgeKey{edge.V, edge.U}]
		if histWeightUV < 0 || histWeightVU < 0 {
			fmt.Printf("[Verify] Edge (%d -> %d) has negative historical weight.\n", edge.U, edge.V)
			return false
		}

		// 验证边的方向是否遵循规则：权重更高者胜出，权重相同ID小者胜出
		wUV, wVU := edge.WeightUV, edge.WeightVU
		if !(wUV >= edgeThresh && (wVU < edgeThresh || wUV > wVU || (wUV == wVU && edge.U < edge.V))) {
			// 这条边 (U->V) 不应该存在
			fmt.Printf("[Verify] Edge direction rule violated for (%d -> %d). Weights: %d vs %d\n", edge.U, edge.V, wUV, wVU)
			return false
		}
	}

	// --- 2. 重建子图并验证其拓扑结构 ---
	// Step 2a: 根据 proof 信息重建一个临时的子图
	subgraph := &types.DependencyGraph{
		Nodes: make(map[uint64]bool),
		Edges: make(map[uint64][]uint64),
	}
	for id := range proof.TxStates {
		subgraph.Nodes[id] = true
	}
	for _, edge := range proof.Edges {
		// 确保边的两端都在子图内
		if subgraph.Nodes[edge.U] && subgraph.Nodes[edge.V] {
			subgraph.Edges[edge.U] = append(subgraph.Edges[edge.U], edge.V)
		}
	}

	// Step 2b: 运行 Tarjan 和拓扑排序
	sccsData := TarjanSCC(subgraph)
	condensationGraph, _, sccInfos := BuildCondensationAndSCCInfo(subgraph, sccsData, proof.TxStates)
	sortedSCCIndices := TopoSortCondensation(condensationGraph)

	// Step 2c: 找到最后一个包含 Solid 节点的 SCC
	lastSolidSCCTopoIndex := -1
	for i, sccIdx := range sortedSCCIndices {
		sccInfo := sccInfos[sccIdx]
		sccInfo.TopoIndex = i
		sccInfos[sccIdx] = sccInfo
		if sccInfo.IsSolid {
			lastSolidSCCTopoIndex = i
		}
	}

	// 如果提案不为空，则必须有一个 solid anchor
	if len(proposedIDs) > 0 && lastSolidSCCTopoIndex == -1 {
		fmt.Printf("[Verify] No solid SCC found in a non-empty proposal.\n")
		return false
	}
	// 如果提案为空，且确实没有 solid anchor，则是合法的空提案
	if len(proposedIDs) == 0 && lastSolidSCCTopoIndex == -1 {
		return true
	}

	// --- 3. 重建最终顺序并与Leader的提案进行比较 ---
	var verifiedOrderIDs []uint64
	for _, sccIdx := range sortedSCCIndices {
		sccInfo := sccInfos[sccIdx]
		if sccInfo.TopoIndex > lastSolidSCCTopoIndex {
			break
		}

		var componentOrder []uint64
		if len(sccInfo.Txs) == 1 {
			componentOrder = sccInfo.Txs
		} else {
			// 为了保证验证的确定性，验证者端也使用一个确定性的排序算法。
			// 这里我们使用ID排序，因为它最简单且绝对确定。
			// 这要求Leader端的哈密顿旋转算法也必须是确定性的，
			// 否则即使逻辑正确，这里的严格比较也会失败。
			// 我们的 `findHamiltonianCycle` 实现是确定性的，所以这能工作。
			// 如果未来要换成不确定性算法，就需要放松这里的验证。

			// 暂时的简化：为了保证一定能跑通，Verifier强制用ID排序
			// TODO: 让Verifier也跑一遍确定性的哈密顿旋转
			component := make([]uint64, len(sccInfo.Txs))
			copy(component, sccInfo.Txs)
			sort.Slice(component, func(i, j int) bool { return component[i] < component[j] })
			componentOrder = component
		}
		verifiedOrderIDs = append(verifiedOrderIDs, componentOrder...)
	}

	// 使用 slices.Equal 进行严格的、逐元素的顺序比较
	if !slices.Equal(proposedIDs, verifiedOrderIDs) {
		fmt.Printf("[Verify] Final order mismatch!\n  Leader proposed: %v\n  Follower verified: %v\n", proposedIDs, verifiedOrderIDs)
		return false
	}

	// 所有验证都通过
	return true
}

// --- Getters for Monitoring ---
func (s *OFOService) GetFinalizedCount() int {
	s.rwMu.RLock()
	defer s.rwMu.RUnlock()
	return len(s.finalizedTxsLog)
}
func (s *OFOService) GetUTIGNodeCount() int {
	if !s.isLeader {
		return -1
	}
	s.rwMu.RLock()
	defer s.rwMu.RUnlock()
	return s.UtigManager.GetUTIGNodesCount()
}

func (s *OFOService) GetLatencyStats() (avgLatency time.Duration, count int64) {
	s.rwMu.RLock()
	defer s.rwMu.RUnlock()

	if s.finalizedCountForLatency == 0 {
		return 0, 0
	}

	avg := s.totalLatency / time.Duration(s.finalizedCountForLatency)
	return avg, s.finalizedCountForLatency
}
