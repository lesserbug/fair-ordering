// pkg/ofo/dependency.go
package ofo

import (
	"SpeedFair_simplify/pkg/types"
	"bytes"
	"math"
	"sort"
)

type WeightMatrix map[[32]byte]map[[32]byte]int

// DependencyManager 封装了Themis的依赖图计算逻辑
type DependencyManager struct {
	graph    *types.DependencyGraph
	txStates map[[32]byte]types.TxState
	weights  WeightMatrix
}

func NewDependencyManager() *DependencyManager {
	return &DependencyManager{
		graph: &types.DependencyGraph{
			Nodes: make(map[[32]byte]bool),
			Edges: make(map[[32]byte][][32]byte),
		},
		txStates: make(map[[32]byte]types.TxState),
		weights:  make(WeightMatrix),
	}
}

func addEdge(g *types.DependencyGraph, u, v [32]byte) {
	g.Edges[u] = append(g.Edges[u], v)
}

// BuildGraphAndClassifyTxs (FairPropose) 从本地排序构建新交易的依赖图
func (dm *DependencyManager) BuildGraphAndClassifyTxs(
	orders []*types.LocalOrder,
	n, f uint64,
	gamma float64,
	committedTxIDs map[[32]byte]bool,
) bool {
	uniqueTxsInOrders := make(map[[32]byte]bool)
	filteredOrders := make([]*types.LocalOrder, 0, len(orders))
	for _, order := range orders {
		pendingTxs := make([][32]byte, 0, len(order.OrderedTxs))
		for _, txID := range order.OrderedTxs {
			if !committedTxIDs[txID] {
				pendingTxs = append(pendingTxs, txID)
				uniqueTxsInOrders[txID] = true
			}
		}
		if len(pendingTxs) > 0 {
			filteredOrders = append(filteredOrders, &types.LocalOrder{
				ReplicaID:  order.ReplicaID,
				OrderedTxs: pendingTxs,
			})
		}
	}

	if len(uniqueTxsInOrders) == 0 {
		return false
	}

	// ** 老师提到的阈值计算，使用浮点数以保证精度 **
	// non-blank/edge threshold: ceil(n(1-γ) + fγ + 1)
	// 在你的例子 n=21, f=5, gamma=0.9: 21*0.1 + 5*0.9 + 1 = 2.1 + 4.5 + 1 = 7.6 -> ceil(7.6) = 8
	// 你的实现里直接用int乘法可能有精度损失，但结果可能是对的。这里用浮点数更严谨。
	edgeThresholdF := float64(n)*(1.0-gamma) + float64(f)*gamma + 1.0
	edgeThreshold := int(math.Ceil(edgeThresholdF))

	solidThreshold := int(n - 2*f)
	if solidThreshold < 1 {
		solidThreshold = 1
	}

	txCounts := make(map[[32]byte]int)
	for _, order := range filteredOrders {
		seenInOrder := make(map[[32]byte]bool)
		for _, tx := range order.OrderedTxs {
			if !seenInOrder[tx] {
				txCounts[tx]++
				seenInOrder[tx] = true
			}
		}
	}

	dm.graph.Nodes = make(map[[32]byte]bool)
	dm.txStates = make(map[[32]byte]types.TxState)
	for tx := range uniqueTxsInOrders {
		count := txCounts[tx]
		state := types.StateBlank
		if count >= edgeThreshold {
			if count >= solidThreshold {
				state = types.StateSolid
			} else {
				state = types.StateShaded
			}
		}
		dm.txStates[tx] = state
		if state != types.StateBlank {
			dm.graph.Nodes[tx] = true
		}
	}

	if len(dm.graph.Nodes) == 0 {
		return false
	}

	dm.weights = make(WeightMatrix)
	for tx1 := range dm.graph.Nodes {
		dm.weights[tx1] = make(map[[32]byte]int)
	}
	for _, order := range filteredOrders {
		txList := order.OrderedTxs
		for i := 0; i < len(txList); i++ {
			tx1 := txList[i]
			if _, ok := dm.graph.Nodes[tx1]; !ok {
				continue
			}
			for j := i + 1; j < len(txList); j++ {
				tx2 := txList[j]
				if _, ok := dm.graph.Nodes[tx2]; !ok {
					continue
				}
				dm.weights[tx1][tx2]++
			}
		}
	}

	dm.graph.Edges = make(map[[32]byte][][32]byte)
	sortedNodes := make([][32]byte, 0, len(dm.graph.Nodes))
	for node := range dm.graph.Nodes {
		sortedNodes = append(sortedNodes, node)
	}
	sort.Slice(sortedNodes, func(i, j int) bool {
		return bytes.Compare(sortedNodes[i][:], sortedNodes[j][:]) < 0
	})

	for i := 0; i < len(sortedNodes); i++ {
		u := sortedNodes[i]
		for j := i + 1; j < len(sortedNodes); j++ {
			v := sortedNodes[j]
			wUV := dm.weights[u][v]
			wVU := dm.weights[v][u]

			if wUV >= edgeThreshold && wVU < edgeThreshold {
				addEdge(dm.graph, u, v)
			} else if wVU >= edgeThreshold && wUV < edgeThreshold {
				addEdge(dm.graph, v, u)
			} else if wUV >= edgeThreshold && wVU >= edgeThreshold {
				if wUV > wVU {
					addEdge(dm.graph, u, v)
				} else if wVU > wUV {
					addEdge(dm.graph, v, u)
				} else {
					if bytes.Compare(u[:], v[:]) < 0 {
						addEdge(dm.graph, u, v)
					} else {
						addEdge(dm.graph, v, u)
					}
				}
			}
		}
	}
	return true
}

// *** NEW FUNCTION: CutShadedTail (The "last-solid cut") ***
func (dm *DependencyManager) CutShadedTail() {
	if len(dm.graph.Nodes) == 0 {
		return
	}

	// 1. Build Condensation Graph and get SCC info
	sccsData := tarjanSCC(dm.graph)
	condensationGraph, _, sccInfos := dm.buildCondensationAndSCCInfo(sccsData)
	sortedSCCIndices := topoSortCondensation(condensationGraph)

	// 2. Find the index of the last SCC that contains a solid transaction
	lastSolidSCCIndex := -1
	for i := len(sortedSCCIndices) - 1; i >= 0; i-- {
		sccID := sortedSCCIndices[i]
		if sccInfos[sccID].IsSolid {
			lastSolidSCCIndex = i
			break
		}
	}

	// If no solid SCC exists, we can propose an empty graph to maintain liveness.
	// Or, more simply for now, just keep all nodes as is. Let's keep them.
	if lastSolidSCCIndex == -1 {
		return
	}

	// 3. Identify all nodes to keep
	nodesToKeep := make(map[[32]byte]bool)
	for i := 0; i <= lastSolidSCCIndex; i++ {
		sccID := sortedSCCIndices[i]
		for _, tx := range sccInfos[sccID].Txs {
			nodesToKeep[tx] = true
		}
	}

	// 4. Prune the graph: remove nodes and their associated edges
	newGraphNodes := make(map[[32]byte]bool)
	newGraphEdges := make(map[[32]byte][][32]byte)
	newTxStates := make(map[[32]byte]types.TxState)

	for node := range dm.graph.Nodes {
		if nodesToKeep[node] {
			newGraphNodes[node] = true
			newTxStates[node] = dm.txStates[node]

			// Keep edges where both source and destination are kept
			var newEdgesForNode [][32]byte
			if originalEdges, ok := dm.graph.Edges[node]; ok {
				for _, neighbor := range originalEdges {
					if nodesToKeep[neighbor] {
						newEdgesForNode = append(newEdgesForNode, neighbor)
					}
				}
			}
			if len(newEdgesForNode) > 0 {
				newGraphEdges[node] = newEdgesForNode
			}
		}
	}

	dm.graph.Nodes = newGraphNodes
	dm.graph.Edges = newGraphEdges
	dm.txStates = newTxStates
}

func FairUpdate(
	updateOrders []*types.UpdateOrder,
	deferredGraph *types.DependencyGraph,
	n, f uint64,
	gamma float64,
) map[[32]byte][][32]byte {
	edgeThresholdF := float64(n)*(1.0-gamma) + float64(f)*gamma + 1.0
	edgeThreshold := int(math.Ceil(edgeThresholdF))

	txsInGraph := make([][32]byte, 0, len(deferredGraph.Nodes))
	for tx := range deferredGraph.Nodes {
		txsInGraph = append(txsInGraph, tx)
	}
	sort.Slice(txsInGraph, func(i, j int) bool { return bytes.Compare(txsInGraph[i][:], txsInGraph[j][:]) < 0 })

	pairsToUpdate := make(map[[32]byte]map[[32]byte]bool)
	for i := 0; i < len(txsInGraph); i++ {
		u := txsInGraph[i]
		for j := i + 1; j < len(txsInGraph); j++ {
			v := txsInGraph[j]
			uHasV, vHasU := false, false
			if edges, ok := deferredGraph.Edges[u]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], v[:]) {
						uHasV = true
						break
					}
				}
			}
			if edges, ok := deferredGraph.Edges[v]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], u[:]) {
						vHasU = true
						break
					}
				}
			}
			if !(uHasV || vHasU) {
				if pairsToUpdate[u] == nil {
					pairsToUpdate[u] = make(map[[32]byte]bool)
				}
				pairsToUpdate[u][v] = true
			}
		}
	}

	if len(pairsToUpdate) == 0 {
		return nil
	}

	weights := make(WeightMatrix)
	for _, order := range updateOrders {
		txList := order.OrderedTxs
		txPos := make(map[[32]byte]int, len(txList))
		for i, tx := range txList {
			txPos[tx] = i
		}
		for u, vMap := range pairsToUpdate {
			for v := range vMap {
				posU, okU := txPos[u]
				posV, okV := txPos[v]
				if okU && okV {
					if weights[u] == nil {
						weights[u] = make(map[[32]byte]int)
					}
					if weights[v] == nil {
						weights[v] = make(map[[32]byte]int)
					}
					if posU < posV {
						weights[u][v]++
					} else {
						weights[v][u]++
					}
				}
			}
		}
	}

	newEdges := make(map[[32]byte][][32]byte)
	for u, vMap := range pairsToUpdate {
		for v := range vMap {
			wUV := weights[u][v]
			wVU := weights[v][u]
			if wUV >= edgeThreshold && wVU < edgeThreshold {
				newEdges[u] = append(newEdges[u], v)
			} else if wVU >= edgeThreshold && wUV < edgeThreshold {
				newEdges[v] = append(newEdges[v], u)
			} else if wUV >= edgeThreshold && wVU >= edgeThreshold {
				if wUV > wVU {
					newEdges[u] = append(newEdges[u], v)
				} else if wVU > wUV {
					newEdges[v] = append(newEdges[v], u)
				} else {
					if bytes.Compare(u[:], v[:]) < 0 {
						newEdges[u] = append(newEdges[u], v)
					} else {
						newEdges[v] = append(newEdges[v], u)
					}
				}
			}
		}
	}
	return newEdges
}

func IsTournament(g *types.DependencyGraph) bool {
	if g == nil || len(g.Nodes) <= 1 {
		return true
	}
	nodes := make([][32]byte, 0, len(g.Nodes))
	for node := range g.Nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return bytes.Compare(nodes[i][:], nodes[j][:]) < 0 })

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			u, v := nodes[i], nodes[j]
			edgeUV, edgeVU := false, false
			if edges, ok := g.Edges[u]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], v[:]) {
						edgeUV = true
						break
					}
				}
			}
			if edges, ok := g.Edges[v]; ok {
				for _, neighbor := range edges {
					if bytes.Equal(neighbor[:], u[:]) {
						edgeVU = true
						break
					}
				}
			}
			if !(edgeUV != edgeVU) {
				return false
			}
		}
	}
	return true
}

func (dm *DependencyManager) ComputeFairOrder() [][32]byte {
	if !IsTournament(dm.graph) {
		return nil
	}

	if len(dm.graph.Nodes) == 0 {
		return nil
	}

	sccsData := tarjanSCC(dm.graph)
	condensationGraph, _, sccInfos := dm.buildCondensationAndSCCInfo(sccsData)
	sortedSCCIndices := topoSortCondensation(condensationGraph)

	var finalOrder [][32]byte
	for _, sccIdx := range sortedSCCIndices {
		sccInfo := sccInfos[sccIdx]
		componentTxs := sccInfo.Txs
		if len(componentTxs) == 0 {
			continue
		}
		sort.Slice(componentTxs, func(i, j int) bool {
			return bytes.Compare(componentTxs[i][:], componentTxs[j][:]) < 0
		})
		finalOrder = append(finalOrder, componentTxs...)
	}
	return finalOrder
}

func (dm *DependencyManager) buildCondensationAndSCCInfo(sccsData [][][32]byte) (map[int][]int, map[[32]byte]int, map[int]types.SCCInfo) {
	nodeToSCCIndex := make(map[[32]byte]int)
	sccInfos := make(map[int]types.SCCInfo)
	for i, compTxs := range sccsData {
		isSolidSCC := false
		for _, node := range compTxs {
			nodeToSCCIndex[node] = i
			if dm.txStates[node] == types.StateSolid {
				isSolidSCC = true
			}
		}
		sccInfos[i] = types.SCCInfo{ID: i, Txs: compTxs, IsSolid: isSolidSCC}
	}

	condensationGraph := make(map[int][]int)
	for i := range sccsData {
		condensationGraph[i] = []int{}
	}

	for u, neighbors := range dm.graph.Edges {
		sccUIndex := nodeToSCCIndex[u]
		for _, v := range neighbors {
			sccVIndex := nodeToSCCIndex[v]
			if sccUIndex != sccVIndex {
				exists := false
				for _, existingNeighbor := range condensationGraph[sccUIndex] {
					if existingNeighbor == sccVIndex {
						exists = true
						break
					}
				}
				if !exists {
					condensationGraph[sccUIndex] = append(condensationGraph[sccUIndex], sccVIndex)
				}
			}
		}
	}
	return condensationGraph, nodeToSCCIndex, sccInfos
}

func tarjanSCC(graph *types.DependencyGraph) [][][32]byte {
	index, indices, lowlink := 0, make(map[[32]byte]int), make(map[[32]byte]int)
	var stack [][32]byte
	onStack := make(map[[32]byte]bool)
	var sccs [][][32]byte

	var strongConnect func(v [32]byte)
	strongConnect = func(v [32]byte) {
		indices[v], lowlink[v] = index, index
		index++
		stack = append(stack, v)
		onStack[v] = true

		var sortedNeighbors [][32]byte
		if neighbors, ok := graph.Edges[v]; ok {
			sortedNeighbors = make([][32]byte, len(neighbors))
			copy(sortedNeighbors, neighbors)
			sort.Slice(sortedNeighbors, func(i, j int) bool { return bytes.Compare(sortedNeighbors[i][:], sortedNeighbors[j][:]) < 0 })
		}

		for _, w := range sortedNeighbors {
			if _, visited := indices[w]; !visited {
				strongConnect(w)
				lowlink[v] = min(lowlink[v], lowlink[w])
			} else if onStack[w] {
				lowlink[v] = min(lowlink[v], indices[w])
			}
		}

		if lowlink[v] == indices[v] {
			var currentSCC [][32]byte
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				currentSCC = append(currentSCC, w)
				if bytes.Equal(w[:], v[:]) {
					break
				}
			}
			sort.Slice(currentSCC, func(i, j int) bool { return bytes.Compare(currentSCC[i][:], currentSCC[j][:]) < 0 })
			sccs = append(sccs, currentSCC)
		}
	}

	nodesToVisit := make([][32]byte, 0, len(graph.Nodes))
	for node := range graph.Nodes {
		nodesToVisit = append(nodesToVisit, node)
	}
	sort.Slice(nodesToVisit, func(i, j int) bool { return bytes.Compare(nodesToVisit[i][:], nodesToVisit[j][:]) < 0 })

	for _, v := range nodesToVisit {
		if _, visited := indices[v]; !visited {
			strongConnect(v)
		}
	}
	sort.Slice(sccs, func(i, j int) bool { return bytes.Compare(sccs[i][0][:], sccs[j][0][:]) < 0 })
	return sccs
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func topoSortCondensation(condensation map[int][]int) []int {
	inDegree, allNodes := make(map[int]int), make(map[int]bool)
	for u, neighbors := range condensation {
		allNodes[u] = true
		for _, v := range neighbors {
			allNodes[v] = true
			inDegree[v]++
		}
	}

	nodeList := make([]int, 0, len(allNodes))
	for node := range allNodes {
		nodeList = append(nodeList, node)
	}
	sort.Ints(nodeList)

	var queue []int
	for _, u := range nodeList {
		if inDegree[u] == 0 {
			queue = append(queue, u)
		}
	}

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
