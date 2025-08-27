// pkg/types/types.go
package types

import (
	"time"
)

// Transaction - 模拟的交易，增加了提交时间戳用于计算延迟
type Transaction struct {
	ID             [32]byte
	SubmissionTime time.Time
}

// PoolTx - 用于在内存池中对交易进行排序的辅助结构
type PoolTx struct {
	ID   [32]byte
	Time time.Time
}

// TxState - 交易在图中的状态 (Solid, Shaded, Blank)
type TxState string

const (
	StateSolid  TxState = "solid"
	StateShaded TxState = "shaded"
	StateBlank  TxState = "blank"
)

// LocalOrder - 节点本地对【新】交易的排序结果
type LocalOrder struct {
	ReplicaID  uint64
	OrderedTxs [][32]byte // 按接收顺序排列的交易哈希
}

// UpdateOrder - 节点本地对【延迟】交易的排序结果
type UpdateOrder struct {
	BlockHeight uint64
	OrderedTxs  [][32]byte
}

// ReplicaOrders - 节点发送给 Leader 的完整排序信息
type ReplicaOrders struct {
	ReplicaID   uint64
	NewTxOrder  *LocalOrder    // 对 txPool 中新交易的排序
	DeferredTxs []*UpdateOrder // 对过去延迟的区块中交易的排序
}

// DependencyGraph - Themis论文中定义的依赖图 G
type DependencyGraph struct {
	Nodes map[[32]byte]bool       // 图中的节点 (non-blank txs)
	Edges map[[32]byte][][32]byte // 记录交易间的依赖关系 u -> [v1, v2...]
}

// SCCInfo - 强连通分量的信息，用于排序和剪枝
type SCCInfo struct {
	ID      int
	Txs     [][32]byte
	IsSolid bool
}

// LocalOrderFragment - 作为验证证据(π)的数据结构
type LocalOrderFragment struct {
	ReplicaOrders []*ReplicaOrders // n-f个节点的完整排序信息
}

// LeaderProposal - 代表 Leader 的完整提案 B = (G, E_updates, π)
type LeaderProposal struct {
	BlockHeight    uint64
	Graph          *DependencyGraph                   // 新交易的依赖图 G_new
	Updates        map[uint64]map[[32]byte][][32]byte // 对旧区块的更新 E_updates, map[blockHeight]edges
	Proof          *LocalOrderFragment                // 用于验证的证据 π
	CommittedSoFar map[[32]byte]bool                  // Leader 计算此提案时所基于的 committed 集合快照
	TxStates       map[[32]byte]TxState               // Leader为新图计算出的交易状态
}
