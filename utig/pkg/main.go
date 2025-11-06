package main

import (
	"SpeedFair_simplify/pkg/network"
	ofo "SpeedFair_simplify/pkg/ofo"
	"SpeedFair_simplify/pkg/types"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"flag" // <<< 确保 'flag' 被导入
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func init() {
	gob.Register(&types.Transaction{})
	gob.Register(&types.LocalOrder{})
	gob.Register(&types.VerifiableFairOrderFragment{})
}

// --- 将这些常量作为命令行标志的默认值 ---
const (
	TOTAL_TRANSACTIONS_DEFAULT         = 20000
	TX_SUBMISSION_RATE_PER_SEC_DEFAULT = 700
	LO_GENERATION_INTERVAL_MS_DEFAULT  = 150
	LO_MAX_TX_COUNT_DEFAULT            = 200
	SIMULATION_DURATION_SEC_DEFAULT    = 10
	LEADER_REPLICA_ID                  = 0
)

var idRegistry = types.NewTxIDRegistry()

func main() {
	// --- 1. 添加新的命令行标志，并使用常量作为默认值 ---
	var (
		configFile = flag.String("config", "config.json", "JSON config file for node addresses")
		nodeList   = flag.String("nodes", "", "Comma-separated list of node IDs to run on this instance")
		cpuProfile = flag.Bool("cpuprofile", false, "Enable CPU profiling for this instance")
		faultCount = flag.Uint64("f", 2, "Number of tolerated faulty replicas")
		gamma      = flag.Float64("gamma", 0.90, "Fairness parameter gamma")

		// <<< 新增的标志，用于控制批次大小 >>>
		loInterval  = flag.Int("lo-interval", LO_GENERATION_INTERVAL_MS_DEFAULT, "Interval in milliseconds for generating local orders")
		loSize      = flag.Int("lo-size", LO_MAX_TX_COUNT_DEFAULT, "Maximum number of transactions in one LocalOrder")
		txRate      = flag.Int("tx-rate", TX_SUBMISSION_RATE_PER_SEC_DEFAULT, "Transaction submission rate (tx/s)")
		simDuration = flag.Int("sim-duration", SIMULATION_DURATION_SEC_DEFAULT, "Simulation duration in seconds")
	)
	flag.Parse()

	if *cpuProfile {
		f, _ := os.Create(fmt.Sprintf("cpu_profile_nodes_%s.pprof", strings.Replace(*nodeList, ",", "_", -1)))
		if f != nil {
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
	}

	// 从配置文件读取节点信息
	config, totalNodesFromConfig := readJSONConfig(*configFile)
	nodesToRun := parseNodeList(*nodeList)
	if len(nodesToRun) == 0 {
		log.Fatal("No nodes specified. Use -nodes flag.")
	}

	maliciousThresholdID := totalNodesFromConfig - *faultCount

	// 打印时，确保使用从配置文件中读取到的总节点数
	fmt.Printf("Starting UTIG-based nodes: %v\n", nodesToRun)
	// <<< 修正点: 使用 totalNodesFromConfig >>>
	fmt.Printf("System params: N=%d, F=%d, Gamma=%.2f\n", totalNodesFromConfig, *faultCount, *gamma)
	fmt.Printf("Malicious replicas are assumed to be IDs >= %d\n", maliciousThresholdID) // 打印提示信息
	fmt.Printf("Workload params: TxRate=%d/s, LO-Interval=%dms, LO-Size=%d\n", *txRate, *loInterval, *loSize)
	fmt.Printf("Simulation duration: %d seconds\n\n", *simDuration)

	// --- MODIFIED: 创建根上下文和信号监听器 ---
	// 1. 创建一个可以被取消的根 context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保在 main 函数退出时，所有协程都能收到取消信号

	// 2. 启动一个协程来监听中断信号 (Ctrl+C)
	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-shutdownSignal
		log.Printf("\nReceived signal: %s. Initiating graceful shutdown...", sig)
		cancel() // 当收到信号时，调用 cancel() 来通知所有协程关闭
	}()

	// 3. 为了让实验能够自动结束，我们仍然使用超时。
	// 这里我们创建一个带有超时的子 context。
	// 如果用户按了 Ctrl+C，根 context 会先被 cancel。
	// 如果没按，超时后子 context 会被 cancel。
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(*simDuration)*time.Second)
	defer timeoutCancel()

	// 将正确的 totalNodes 和新的 timeoutCtx 传递下去
	runDistributedMode(timeoutCtx, nodesToRun, maliciousThresholdID, config, totalNodesFromConfig, *faultCount, *gamma, *loInterval, *loSize, *txRate)
}

// <<< MODIFIED: 函数签名接收 maliciousThresholdID >>>
func runDistributedMode(ctx context.Context, nodeIDs []uint64, maliciousThresholdID uint64, config map[uint64]string, totalNodes, faultCount uint64, gamma float64, loIntervalMs, loSize, txRate int) {
	var wg sync.WaitGroup

	services := make(map[uint64]*ofo.OFOService)
	var servicesMu sync.Mutex
	var txSubmitterNet network.NetworkInterface
	isLeaderInstance := false
	for _, id := range nodeIDs {
		if id == 0 {
			isLeaderInstance = true
			break
		}
	}

	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			netConfig := network.NetworkConfig{ReplicaID: id, ReplicaAddr: config}
			net, err := network.NewDistributedNetwork(netConfig)
			if err != nil {
				log.Printf("Node %d failed to create network: %v", id, err)
				return
			}
			defer net.Stop()

			log.Printf("Node %d waiting for peers to connect...", id)
			if err := net.WaitForPeers(30 * time.Second); err != nil {
				log.Printf("FATAL: Node %d could not connect to all peers, shutting down. Error: %v", id, err)
				return
			}

			if id == 0 && isLeaderInstance {
				servicesMu.Lock()
				txSubmitterNet = net
				servicesMu.Unlock()
			}

			// --- 关键修改：判断当前节点是否为恶意节点 ---
			isMalicious := id >= maliciousThresholdID
			if isMalicious {
				log.Printf("Node %d is starting as a MALICIOUS replica.", id)
			} else {
				log.Printf("Node %d is starting as an HONEST replica.", id)
			}

			// --- 关键修改：将 isMalicious 标志传递给构造函数 ---
			service := ofo.NewOFOService(id, totalNodes, faultCount, gamma, net, idRegistry, loSize, &loIntervalMs, isMalicious)

			servicesMu.Lock()
			services[id] = service
			servicesMu.Unlock()

			loGenTicker := time.NewTicker(time.Duration(loIntervalMs) * time.Millisecond)
			defer loGenTicker.Stop()

		loGenLoop:
			for {
				select {
				case <-loGenTicker.C:
					service.GenerateAndSendLocalOrder()
				case <-ctx.Done():
					break loGenLoop
				}
			}
			log.Printf("Node %d shutting down...", id)
			service.Stop()
		}(nodeID)
	}

	var submittedTxCount int32
	if isLeaderInstance {
		wg.Add(1) // <<< ADDED: WaitGroup 需要等待这个协程 >>>
		go func() {
			defer wg.Done() // <<< ADDED: 协程结束时通知 WaitGroup >>>
			// 等待网络接口准备好
			for {
				select {
				case <-time.After(100 * time.Millisecond):
					servicesMu.Lock()
					netReady := txSubmitterNet != nil
					servicesMu.Unlock()
					if netReady {
						goto netReady
					}
				case <-ctx.Done(): // 如果在准备好之前就收到了关闭信号
					log.Println("Tx Submitter: shutdown before network was ready.")
					return
				}
			}
		netReady:
warmup := 2*time.Duration(loIntervalMs)*time.Millisecond + 50*time.Millisecond // +50ms 缓冲
                        log.Printf("Tx Submitter: network ready. Full pipeline warm-up %v ...", warmup)

                        select {
                        case <-time.After(warmup): // 等待 Ticker 触发 并且 Collector 也完成首轮收集
                        case <-ctx.Done():
                                log.Println("Tx Submitter: shutdown during warm-up.")
                                return
                        }
			submitTransactions(ctx, txSubmitterNet, totalNodes, txRate, &submittedTxCount)
		}()
	}

	// 监控协程也需要等待
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitorSystem(ctx, services, &servicesMu, isLeaderInstance, &submittedTxCount)
	}()

	wg.Wait()
	fmt.Println("\nAll nodes on this instance have shut down.")
}

// <<< MODIFIED: 保持不变，但现在它的 context 来自于 main >>>
func submitTransactions(ctx context.Context, net network.NetworkInterface, totalNodes uint64, txRate int, submittedCounter *int32) {
	if txRate <= 0 {
		log.Println("Transaction submission rate is 0, no transactions will be submitted.")
		return
	}

	// 计算 ticker 间隔，确保不为0
	interval := time.Second / time.Duration(txRate)
	if interval == 0 {
		interval = time.Nanosecond // 对于非常高的速率，使用最小间隔
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	txCounter := 0
	log.Println("Transaction submission started.")
	for {
		select {
		case <-ticker.C:
			txCounter++
			atomic.AddInt32(submittedCounter, 1)
			txHash := sha256.Sum256([]byte(fmt.Sprintf("tx-%d-%d", txCounter, rand.Int())))

			tx := types.Transaction{
				ID:             txHash,
				SubmissionTime: time.Now(),
			}

			// <<< MODIFIED: 注册现在需要传递整个对象 >>>
			idRegistry.GetOrRegister(tx)

			for i := uint64(0); i < totalNodes; i++ {
				net.Send(network.Message{Type: "Transaction", From: 0, To: i, Payload: &tx})
			}
		case <-ctx.Done():
			log.Println("Transaction submission stopping...")
			return // 先返回，再由 defer ticker.Stop() 清理
		}
	}
}

// <<< MODIFIED: monitorSystem 现在也由 WaitGroup 管理，并监听 context >>>
func monitorSystem(ctx context.Context, services map[uint64]*ofo.OFOService, servicesMu *sync.Mutex, isLeaderInstance bool, submittedCounter *int32) {
	if isLeaderInstance {
		var leaderService *ofo.OFOService
		// 等待 Leader 服务实例被创建
		for {
			select {
			case <-time.After(100 * time.Millisecond):
				servicesMu.Lock()
				leaderService = services[0]
				servicesMu.Unlock()
				if leaderService != nil {
					goto leaderReady
				}
			case <-ctx.Done():
				log.Println("Monitor: shutdown before leader service was ready.")
				return
			}
		}
	leaderReady:
		monitorWithLeader(ctx, leaderService, submittedCounter)
	} else {
		monitorWithoutLeader(ctx, services, servicesMu)
	}
}

func monitorWithLeader(ctx context.Context, leaderService *ofo.OFOService, submittedCounter *int32) {
	startTime := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	log.Println("Leader monitor started.")
	for {
		select {
		case <-ticker.C:
			finalizedCount := leaderService.GetFinalizedCount()
			utigSize := leaderService.GetUTIGNodeCount()
			submittedCount := atomic.LoadInt32(submittedCounter)
			elapsed := time.Since(startTime).Seconds()
			if elapsed < 1 {
				elapsed = 1
			}
			tps := float64(finalizedCount) / elapsed

			// <<< ADDED >>> 获取并格式化延迟数据
			avgLatency, latencyCount := leaderService.GetLatencyStats()
			latencyStr := "N/A"
			if latencyCount > 0 {
				latencyStr = avgLatency.Round(time.Millisecond).String()
			}

			// <<< MODIFIED >>> 在输出中加入延迟
			fmt.Printf("\r[LEADER] Time: %.1fs, Submitted: %d, Finalized: %d, UTIG: %d, TPS: %.2f, Avg Latency: %s",
				elapsed, submittedCount, finalizedCount, utigSize, tps, latencyStr)

		case <-ctx.Done():
			fmt.Println("\nSimulation time ended.")
			// <<< MODIFIED >>> 在最终报告中加入延迟
			finalAvgLatency, _ := leaderService.GetLatencyStats()
			printFinalReport(startTime, leaderService.GetFinalizedCount(), atomic.LoadInt32(submittedCounter), finalAvgLatency)
			return
		}
	}
}

func monitorWithoutLeader(ctx context.Context, services map[uint64]*ofo.OFOService, servicesMu *sync.Mutex) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	log.Println("Follower monitor started.")
	for {
		select {
		case <-ticker.C:
			var report strings.Builder
			report.WriteString("\r[FOLLOWERS] ")
			servicesMu.Lock()
			for id, service := range services {
				finalized := service.GetFinalizedCount()
				report.WriteString(fmt.Sprintf("Node %d: {Finalized: %d} ", id, finalized))
			}
			servicesMu.Unlock()
			fmt.Print(report.String())
		case <-ctx.Done():
			fmt.Println("\nFollower monitor shutting down.")
			return
		}
	}
}

func printFinalReport(startTime time.Time, finalizedCount int, totalSubmitted int32, avgLatency time.Duration) { // <<< MODIFIED >>>
	fmt.Println("\n\n--- FINAL RESULTS ---")
	elapsedSeconds := time.Since(startTime).Seconds()
	if elapsedSeconds < 1 {
		elapsedSeconds = 1
	}
	throughput := float64(finalizedCount) / elapsedSeconds
	fmt.Printf("Total Submitted: %d\n", totalSubmitted)
	fmt.Printf("Total Finalized: %d\n", finalizedCount)
	fmt.Printf("Average TPS:     %.2f\n", throughput)

	// <<< ADDED >>> 打印平均延迟
	latencyStr := "N/A"
	if avgLatency > 0 {
		latencyStr = avgLatency.Round(time.Millisecond).String()
	}
	fmt.Printf("Average Latency: %s\n", latencyStr)
}

type Config struct {
	Nodes map[string]string `json:"nodes"`
}

func readJSONConfig(filename string) (map[uint64]string, uint64) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read config file %s: %v", filename, err)
	}
	var jsonConfig Config
	if err := json.Unmarshal(data, &jsonConfig); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}
	config := make(map[uint64]string)
	var maxID uint64 = 0
	for k, v := range jsonConfig.Nodes {
		id, _ := strconv.ParseUint(k, 10, 64)
		if id > maxID {
			maxID = id
		}
		config[id] = v
	}
	return config, maxID + 1
}
func parseNodeList(nodeList string) []uint64 {
	if nodeList == "" {
		return []uint64{}
	}
	parts := strings.Split(nodeList, ",")
	nodes := make([]uint64, 0, len(parts))
	for _, part := range parts {
		id, _ := strconv.ParseUint(strings.TrimSpace(part), 10, 64)
		nodes = append(nodes, id)
	}
	return nodes
}
