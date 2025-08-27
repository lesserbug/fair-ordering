// pkg/types/types.go
package types

import (
	"math"
	"sort" // <<< 新增导入
	"sync"
	"time" // <<< 新增导入
)

// Transaction - 模拟的交易，只包含哈希
type Transaction struct {
	ID             [32]byte
	SubmissionTime time.Time // <<< 新增字段：交易提交时间戳
}

// TxIDRegistry - 在内存中将交易哈希映射到一个唯一的、更小的uint64 ID。
// 这可以大大减小图结构和网络消息的大小。
type TxIDRegistry struct {
	mu             sync.Mutex
	hashToID       map[[32]byte]uint64
	idToHash       map[uint64][32]byte
	idToSubmitTime map[uint64]time.Time // <<< ADDED: 用于稳定排序
	nextID         uint64
}

func NewTxIDRegistry() *TxIDRegistry {
	return &TxIDRegistry{
		hashToID:       make(map[[32]byte]uint64),
		idToHash:       make(map[uint64][32]byte),
		idToSubmitTime: make(map[uint64]time.Time), // <<< ADDED
		nextID:         1,                          // 从1开始，0可以作为保留值
	}
}

// <<< MODIFIED: 注册时同时记录时间 >>>
func (r *TxIDRegistry) GetOrRegister(tx Transaction) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 使用 tx.ID 来查找
	if id, exists := r.hashToID[tx.ID]; exists {
		return id
	}

	id := r.nextID
	r.hashToID[tx.ID] = id
	r.idToHash[id] = tx.ID
	r.idToSubmitTime[id] = tx.SubmissionTime // 使用 tx.SubmissionTime
	r.nextID++
	return id
}

// GetHash - 根据ID查找哈希
func (r *TxIDRegistry) GetHash(id uint64) ([32]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hash, ok := r.idToHash[id]
	return hash, ok
}

// GetAllHashes - 获取所有已注册的哈希（用于测试和本地排序生成）
func (r *TxIDRegistry) GetAllHashes() [][32]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	hashes := make([][32]byte, 0, len(r.idToHash))
	for _, hash := range r.idToHash {
		hashes = append(hashes, hash)
	}
	return hashes
}

// TxState - 交易在图中的状态 (Solid, Shaded, Blank)
type TxState string

const (
	StateSolid  TxState = "solid"
	StateShaded TxState = "shaded"
	StateBlank  TxState = "blank"
)

// LocalOrder - 节点本地对交易的排序，使用uint64 ID
type LocalOrder struct {
	ReplicaID  uint64
	OrderedTxs []uint64
}

// DependencyGraph - 用于Tarjan算法的图结构，使用uint64 ID
type DependencyGraph struct {
	Nodes map[uint64]bool
	Edges map[uint64][]uint64
}

// FairOrderFragment - Leader提出的已定序交易片段
type FairOrderFragment struct {
	OrderedTxIDs    []uint64
	OrderedTxHashes [][32]byte
}

// ProofEdge - 证明一条边的权重断言
type ProofEdge struct {
	U, V     uint64
	WeightUV int
	WeightVU int
}

// ProofData - 验证一个FinalOrder所需的所有信息
type ProofData struct {
	TxStates map[uint64]TxState
	Edges    []ProofEdge
}

// VerifiableFairOrderFragment - Leader广播的完整提案，包含最终排序、源数据和证明
type VerifiableFairOrderFragment struct {
	FinalOrder        *FairOrderFragment
	SourceLocalOrders []*LocalOrder
	Proof             *ProofData
}

// SCCInfo - 强连通分量的信息
type SCCInfo struct {
	ID        int
	Txs       []uint64
	IsSolid   bool
	TopoIndex int
}

// CalculateEdgeThreshold 计算建立边和非空白状态所需的最小支持数
func CalculateEdgeThreshold(n, f uint64, gamma float64) int {
	if gamma == 1.0 {
		return int(f + 1)
	}
	thresh := int(math.Floor(float64(n)*(1.0-gamma) + gamma*float64(f) + 1.0))
	if thresh < 1 {
		return 1
	}
	return thresh
}

// CalculateSolidThreshold 计算成为Solid状态所需的最小支持数
func CalculateSolidThreshold(n, f uint64) int {
	thresh := int(n - 2*f)
	if thresh < 1 {
		return 1
	}
	return thresh
}

func (r *TxIDRegistry) GetUnfinalizedIDsSortedByTime(finalizedLogs map[[32]byte]bool) []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	unfinalized := make([]uint64, 0, len(r.idToHash))
	for id, hash := range r.idToHash {
		if !finalizedLogs[hash] {
			unfinalized = append(unfinalized, id)
		}
	}

	// 按提交时间排序
	sort.Slice(unfinalized, func(i, j int) bool {
		return r.idToSubmitTime[unfinalized[i]].Before(r.idToSubmitTime[unfinalized[j]])
	})

	return unfinalized
}
