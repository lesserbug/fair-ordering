package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"sort"
)

type edgeKey struct {
	u, v uint64
}

// DependencyManager - 你的UTIG管理器
type DependencyManager struct {
	nodes         map[uint64]bool
	txStates      map[uint64]types.TxState
	weights       map[edgeKey]int
	adjacency     map[uint64]map[uint64]bool
	edges         map[uint64]map[uint64]bool
	inverseEdges  map[uint64]map[uint64]bool
	lastSeenRound map[uint64]int

	nReplicas uint64
	fFaulty   uint64
	gammaVal  float64

	solidThresholdCount           int
	edgeAndNonBlankThresholdCount int
}

func NewDependencyManager(n, f uint64, gamma float64) *DependencyManager {
	dm := &DependencyManager{
		nodes:         make(map[uint64]bool),
		txStates:      make(map[uint64]types.TxState),
		weights:       make(map[edgeKey]int),
		adjacency:     make(map[uint64]map[uint64]bool),
		edges:         make(map[uint64]map[uint64]bool),
		inverseEdges:  make(map[uint64]map[uint64]bool),
		lastSeenRound: make(map[uint64]int),
		nReplicas:     n,
		fFaulty:       f,
		gammaVal:      gamma,
	}

	// <<< MODIFIED: 使用公共函数计算阈值 >>>
	dm.solidThresholdCount = types.CalculateSolidThreshold(n, f)
	dm.edgeAndNonBlankThresholdCount = types.CalculateEdgeThreshold(n, f, gamma)

	return dm
}

// <<< MODIFIED: 增强的剪枝逻辑 >>>
// PruneStaleTxs - 移除长时间未被提及的非Solid交易
func (dm *DependencyManager) PruneStaleTxs(currentRound int, staleThreshold int) {
	var staleTxIDs []uint64
	for txID, lastSeen := range dm.lastSeenRound {
		// 如果一个交易已经超过了staleThreshold个轮次没有被任何LocalOrder提及
		if (currentRound - lastSeen) > staleThreshold {
			// 并且它不是一个Solid状态的锚点 (这是关键)
			// Shaded和Blank状态的陈旧交易都将被清理
			if dm.txStates[txID] != types.StateSolid {
				staleTxIDs = append(staleTxIDs, txID)
			}
		}
	}

	if len(staleTxIDs) > 0 {
		dm.RemoveTxsFromUTIG(staleTxIDs)
	}
}

func (dm *DependencyManager) GenerateProof(finalizedTxIDs []uint64) *types.ProofData {
	proof := &types.ProofData{
		TxStates: make(map[uint64]types.TxState),
		Edges:    make([]types.ProofEdge, 0),
	}
	finalizedSet := make(map[uint64]bool)
	for _, id := range finalizedTxIDs {
		finalizedSet[id] = true
	}

	for txID := range finalizedSet {
		proof.TxStates[txID] = dm.txStates[txID]
		if children, ok := dm.edges[txID]; ok {
			for childID := range children {
				if finalizedSet[childID] {
					edge := types.ProofEdge{
						U:        txID,
						V:        childID,
						WeightUV: dm.weights[edgeKey{txID, childID}],
						WeightVU: dm.weights[edgeKey{childID, txID}],
					}
					proof.Edges = append(proof.Edges, edge)
				}
			}
		}
	}
	return proof
}

func (dm *DependencyManager) AddTxToUTIG(txID uint64) {
	if _, exists := dm.nodes[txID]; !exists {
		dm.nodes[txID] = true
		dm.txStates[txID] = types.StateBlank
		dm.adjacency[txID] = make(map[uint64]bool)
		dm.edges[txID] = make(map[uint64]bool)
		dm.inverseEdges[txID] = make(map[uint64]bool)
	}
}

func (dm *DependencyManager) RemoveTxsFromUTIG(confirmedTxs []uint64) {
	for _, txID := range confirmedTxs {
		if !dm.nodes[txID] {
			continue
		}
		if neighbors, ok := dm.adjacency[txID]; ok {
			for neighborID := range neighbors {
				delete(dm.weights, edgeKey{txID, neighborID})
				delete(dm.weights, edgeKey{neighborID, txID})
				if dm.adjacency[neighborID] != nil {
					delete(dm.adjacency[neighborID], txID)
				}
			}
		}
		if parents, ok := dm.inverseEdges[txID]; ok {
			for parentID := range parents {
				if dm.edges[parentID] != nil {
					delete(dm.edges[parentID], txID)
				}
			}
		}
		if children, ok := dm.edges[txID]; ok {
			for childID := range children {
				if dm.inverseEdges[childID] != nil {
					delete(dm.inverseEdges[childID], txID)
				}
			}
		}
		delete(dm.nodes, txID)
		delete(dm.txStates, txID)
		delete(dm.adjacency, txID)
		delete(dm.edges, txID)
		delete(dm.inverseEdges, txID)
		delete(dm.lastSeenRound, txID)
	}
}

func (dm *DependencyManager) IncrementalRefreshAndUpdate(localOrders []*types.LocalOrder, currentRound int) {
	stateDirtyNodes := make(map[uint64]bool)
	edgeDirtyPairs := make(map[edgeKey]bool)
	thresh := dm.edgeAndNonBlankThresholdCount

	batchTxSupportCounts := make(map[uint64]int)
	for _, order := range localOrders {
		for _, txID := range order.OrderedTxs {
			if _, ok := dm.nodes[txID]; ok {
				batchTxSupportCounts[txID]++
				dm.lastSeenRound[txID] = currentRound
			}
		}
	}
	for txID, supportCount := range batchTxSupportCounts {
		oldState := dm.txStates[txID]
		newState := types.StateBlank
		if supportCount >= dm.solidThresholdCount {
			newState = types.StateSolid
		} else if supportCount >= dm.edgeAndNonBlankThresholdCount {
			newState = types.StateShaded
		}
		if oldState != newState {
			dm.txStates[txID] = newState
			stateDirtyNodes[txID] = true
		}
	}

	for _, order := range localOrders {
		for i := 0; i < len(order.OrderedTxs); i++ {
			u := order.OrderedTxs[i]
			if !dm.nodes[u] {
				continue
			}
			for j := i + 1; j < len(order.OrderedTxs); j++ {
				v := order.OrderedTxs[j]
				if !dm.nodes[v] {
					continue
				}
				dm.adjacency[u][v] = true
				dm.adjacency[v][u] = true
				keyUV := edgeKey{u, v}
				keyVU := edgeKey{v, u}
				dm.weights[keyUV]++
				if dm.weights[keyUV] >= thresh || dm.weights[keyVU] >= thresh {
					if u < v {
						edgeDirtyPairs[keyUV] = true
					} else {
						edgeDirtyPairs[keyVU] = true
					}
				}
			}
		}
	}

	if len(stateDirtyNodes) > 0 || len(edgeDirtyPairs) > 0 {
		dm.recomputeDirtyEdges(stateDirtyNodes, edgeDirtyPairs)
	}
}

func (dm *DependencyManager) recomputeDirtyEdges(stateDirtyNodes map[uint64]bool, edgeDirtyPairs map[edgeKey]bool) {
	pairsToRecompute := make(map[edgeKey]bool)
	for pair := range edgeDirtyPairs {
		pairsToRecompute[pair] = true
	}
	for u := range stateDirtyNodes {
		if neighbors, ok := dm.adjacency[u]; ok {
			for v := range neighbors {
				if u < v {
					pairsToRecompute[edgeKey{u, v}] = true
				} else {
					pairsToRecompute[edgeKey{v, u}] = true
				}
			}
		}
	}
	for pair := range pairsToRecompute {
		u, v := pair.u, pair.v
		if dm.edges[u] != nil {
			delete(dm.edges[u], v)
		}
		if dm.inverseEdges[v] != nil {
			delete(dm.inverseEdges[v], u)
		}
		if dm.edges[v] != nil {
			delete(dm.edges[v], u)
		}
		if dm.inverseEdges[u] != nil {
			delete(dm.inverseEdges[u], v)
		}
		if dm.txStates[u] == types.StateBlank || dm.txStates[v] == types.StateBlank {
			continue
		}
		wUV := dm.weights[edgeKey{u, v}]
		wVU := dm.weights[edgeKey{v, u}]
		thresh := dm.edgeAndNonBlankThresholdCount
		if wUV >= thresh && (wVU < thresh || wUV > wVU || (wUV == wVU && u < v)) {
			dm.edges[u][v] = true
			dm.inverseEdges[v][u] = true
		} else if wVU >= thresh && (wUV < thresh || wVU > wUV || (wUV == wVU && v < u)) {
			dm.edges[v][u] = true
			dm.inverseEdges[u][v] = true
		}
	}
}

func (dm *DependencyManager) ExtractFairOrder() []uint64 {
	graphForExtraction := &types.DependencyGraph{
		Nodes: make(map[uint64]bool),
		Edges: make(map[uint64][]uint64),
	}
	for id, state := range dm.txStates {
		if state != types.StateBlank {
			graphForExtraction.Nodes[id] = true
		}
	}
	for u, neighbors := range dm.edges {
		if !graphForExtraction.Nodes[u] {
			continue
		}
		graphForExtraction.Edges[u] = make([]uint64, 0, len(neighbors))
		for v := range neighbors {
			if graphForExtraction.Nodes[v] {
				graphForExtraction.Edges[u] = append(graphForExtraction.Edges[u], v)
			}
		}
	}
	if len(graphForExtraction.Nodes) == 0 {
		return nil
	}

	sccsData := TarjanSCC(graphForExtraction)
	condensationGraph, _, sccInfos := BuildCondensationAndSCCInfo(graphForExtraction, sccsData, dm.txStates)
	sortedSCCIndices := TopoSortCondensation(condensationGraph)

	lastSolidSCCTopoIndex := -1
	for i, sccIdx := range sortedSCCIndices {
		sccInfo := sccInfos[sccIdx]
		sccInfo.TopoIndex = i
		sccInfos[sccIdx] = sccInfo
		if sccInfo.IsSolid {
			lastSolidSCCTopoIndex = i
		}
	}
	if lastSolidSCCTopoIndex == -1 {
		return nil
	}

	// <<< ADDED: 准备一个 solid 交易的查找表，传递给排序函数 >>>
	solidTxs := make(map[uint64]bool)
	for txID, state := range dm.txStates {
		if state == types.StateSolid {
			solidTxs[txID] = true
		}
	}

	var finalOrderIDs []uint64
	for _, sccIdx := range sortedSCCIndices {
		sccInfo := sccInfos[sccIdx]
		if sccInfo.TopoIndex > lastSolidSCCTopoIndex {
			break
		}

		if len(sccInfo.Txs) == 1 {
			finalOrderIDs = append(finalOrderIDs, sccInfo.Txs[0])
		} else {
			// <<< TEMPORARY MODIFICATION FOR ROBUST TESTING >>>
			// 暂时使用ID排序，以确保与Verifier的逻辑100%一致
			sortedComponent := make([]uint64, len(sccInfo.Txs))
			copy(sortedComponent, sccInfo.Txs)
			sort.Slice(sortedComponent, func(i, j int) bool { return sortedComponent[i] < sortedComponent[j] })
			finalOrderIDs = append(finalOrderIDs, sortedComponent...)

			// 当需要测试哈密顿旋转时，再换回下面的代码
			// componentOrder := findHamiltonianCycle(sccInfo.Txs, dm.edges, solidTxs)
			// finalOrderIDs = append(finalOrderIDs, componentOrder...)
		}
	}
	return finalOrderIDs
}

// func (dm *DependencyManager) PruneStaleTxs(currentRound int, staleThreshold int) {
// 	var staleTxIDs []uint64
// 	for txID, lastSeen := range dm.lastSeenRound {
// 		if (currentRound-lastSeen) > staleThreshold && dm.txStates[txID] == types.StateBlank {
// 			staleTxIDs = append(staleTxIDs, txID)
// 		}
// 	}
// 	if len(staleTxIDs) > 0 {
// 		dm.RemoveTxsFromUTIG(staleTxIDs)
// 	}
// }

func (dm *DependencyManager) GetUTIGNodesCount() int {
	return len(dm.nodes)
}

// 辅助函数 (Tarjan, TopoSort, etc.)
func BuildCondensationAndSCCInfo(graph *types.DependencyGraph, sccsData [][]uint64, currentTxStates map[uint64]types.TxState) (map[int][]int, map[uint64]int, map[int]types.SCCInfo) {
	nodeToSCCIndex := make(map[uint64]int)
	sccInfos := make(map[int]types.SCCInfo)
	for i, scc := range sccsData {
		isSolid := false
		for _, nodeID := range scc {
			nodeToSCCIndex[nodeID] = i
			if state, ok := currentTxStates[nodeID]; ok && state == types.StateSolid {
				isSolid = true
			}
		}
		sccInfos[i] = types.SCCInfo{ID: i, Txs: scc, IsSolid: isSolid}
	}
	condensationGraph := make(map[int][]int)
	for i := 0; i < len(sccsData); i++ {
		condensationGraph[i] = []int{}
	}
	for u, neighbors := range graph.Edges {
		uSCC := nodeToSCCIndex[u]
		for _, v := range neighbors {
			vSCC := nodeToSCCIndex[v]
			if uSCC != vSCC {
				exists := false
				for _, neighborSCC := range condensationGraph[uSCC] {
					if neighborSCC == vSCC {
						exists = true
						break
					}
				}
				if !exists {
					condensationGraph[uSCC] = append(condensationGraph[uSCC], vSCC)
				}
			}
		}
	}
	return condensationGraph, nodeToSCCIndex, sccInfos
}

func TarjanSCC(graph *types.DependencyGraph) [][]uint64 {
	index := 0
	indices := make(map[uint64]int)
	lowlink := make(map[uint64]int)
	var stack []uint64
	onStack := make(map[uint64]bool)
	var sccs [][]uint64
	var strongConnect func(v uint64)
	strongConnect = func(v uint64) {
		indices[v] = index
		lowlink[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true
		if neighbors, ok := graph.Edges[v]; ok {
			sort.Slice(neighbors, func(i, j int) bool { return neighbors[i] < neighbors[j] })
			for _, w := range neighbors {
				if _, visited := indices[w]; !visited {
					strongConnect(w)
					lowlink[v] = min(lowlink[v], lowlink[w])
				} else if onStack[w] {
					lowlink[v] = min(lowlink[v], indices[w])
				}
			}
		}
		if lowlink[v] == indices[v] {
			var currentSCC []uint64
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				currentSCC = append(currentSCC, w)
				if w == v {
					break
				}
			}
			sort.Slice(currentSCC, func(i, j int) bool { return currentSCC[i] < currentSCC[j] })
			sccs = append(sccs, currentSCC)
		}
	}
	nodesToVisit := make([]uint64, 0, len(graph.Nodes))
	for nodeID := range graph.Nodes {
		nodesToVisit = append(nodesToVisit, nodeID)
	}
	sort.Slice(nodesToVisit, func(i, j int) bool { return nodesToVisit[i] < nodesToVisit[j] })
	for _, v := range nodesToVisit {
		if _, visited := indices[v]; !visited {
			strongConnect(v)
		}
	}
	return sccs
}

func TopoSortCondensation(condensation map[int][]int) []int {
	inDegree := make(map[int]int)
	var allNodes []int
	nodesSet := make(map[int]bool)
	for u, neighbors := range condensation {
		if !nodesSet[u] {
			nodesSet[u] = true
			allNodes = append(allNodes, u)
		}
		for _, v := range neighbors {
			inDegree[v]++
			if !nodesSet[v] {
				nodesSet[v] = true
				allNodes = append(allNodes, v)
			}
		}
	}
	sort.Ints(allNodes)
	var queue []int
	for _, u := range allNodes {
		if inDegree[u] == 0 {
			queue = append(queue, u)
		}
	}
	sort.Ints(queue)
	var sorted []int
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		sorted = append(sorted, u)
		neighbors := condensation[u]
		sort.Ints(neighbors)
		for _, v := range neighbors {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
		sort.Ints(queue)
	}
	return sorted
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// findHamiltonianCycle 尝试在给定的SCC内找到一条哈密顿环路
// sccTxs: SCC内的交易ID列表
// edges: 全局的边信息 (u -> map[v]bool)
// solidTxs: 一个查找表，用于快速判断一个tx是否为solid
func findHamiltonianCycle(sccTxs []uint64, edges map[uint64]map[uint64]bool, solidTxs map[uint64]bool) []uint64 {
	n := len(sccTxs)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return sccTxs
	}

	// --- 1. 确定一个稳定的锚点 (anchor) ---
	// 优先选择 solid 节点，如果没有，则选择 ID 最小的节点作为回退方案
	var anchor uint64 = 0
	minID := sccTxs[0]
	hasSolid := false
	for _, txID := range sccTxs {
		if txID < minID {
			minID = txID
		}
		if solidTxs[txID] {
			anchor = txID
			hasSolid = true
			break // 找到第一个 solid 就用它
		}
	}
	if !hasSolid {
		anchor = minID
	}

	// --- 2. 贪心算法构造哈密顿路径 ---
	path := make([]uint64, 0, n)
	path = append(path, anchor)
	visited := make(map[uint64]bool, n)
	visited[anchor] = true

	current := anchor
	for len(path) < n {
		foundNext := false
		// 寻找一个未被访问过的邻居
		if neighbors, ok := edges[current]; ok {
			// 对邻居进行排序，以获得确定性
			sortedNeighbors := make([]uint64, 0, len(neighbors))
			for neighbor := range neighbors {
				sortedNeighbors = append(sortedNeighbors, neighbor)
			}
			sort.Slice(sortedNeighbors, func(i, j int) bool { return sortedNeighbors[i] < sortedNeighbors[j] })

			for _, neighbor := range sortedNeighbors {
				if !visited[neighbor] {
					// 确保这个邻居确实在当前的SCC内
					is_in_scc := false
					for _, scc_node := range sccTxs {
						if scc_node == neighbor {
							is_in_scc = true
							break
						}
					}

					if is_in_scc {
						current = neighbor
						path = append(path, current)
						visited[current] = true
						foundNext = true
						break
					}
				}
			}
		}
		if !foundNext {
			// 如果贪心失败（不太可能在稠密图中发生，但作为回退）
			// 直接使用 ID 排序，保证程序不会崩溃
			sort.Slice(sccTxs, func(i, j int) bool { return sccTxs[i] < sccTxs[j] })
			return sccTxs
		}
	}

	// --- 3. 旋转路径，使锚点位于末尾 ---
	// path 的形式是 [anchor, node2, node3, ..., last_node]
	// 旋转后应为 [node2, node3, ..., last_node, anchor]
	return append(path[1:], path[0])
}
