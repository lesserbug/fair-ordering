// package main

// import (
// 	"SpeedFair_simplify/pkg/network"
// 	"SpeedFair_simplify/pkg/ofo"
// 	"SpeedFair_simplify/pkg/types"
// 	"context"
// 	"crypto/sha256"
// 	"encoding/gob"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"io/ioutil"
// 	"log"
// 	"math/rand"
// 	"os"
// 	"os/signal"
// 	"runtime/pprof"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"
// 	"syscall"
// 	"time"
// )

// func init() {
// 	gob.Register(&types.Transaction{})
// 	gob.Register(&types.LeaderProposal{})
// 	gob.Register(&types.ReplicaOrders{})
// 	gob.Register(&types.UpdateOrder{})
// 	gob.Register(&types.LocalOrder{})
// 	gob.Register(&types.LocalOrderFragment{})
// 	gob.Register(map[[32]byte]types.TxState{})
// }

// const (
// 	LEADER_REPLICA_ID = 0
// )

// type Config struct {
// 	Nodes map[string]string `json:"nodes"`
// }

// func main() {
// 	var (
// 		configFile  = flag.String("config", "config.json", "JSON config file for node addresses")
// 		nodeList    = flag.String("nodes", "", "Comma-separated list of node IDs to run on this instance")
// 		cpuProfile  = flag.Bool("cpuprofile", false, "Enable CPU profiling for this instance")
// 		faultCount  = flag.Uint64("f", 1, "Number of tolerated faulty replicas")
// 		gamma       = flag.Float64("gamma", 0.90, "Fairness parameter gamma")
// 		loInterval  = flag.Int("lo-interval", 150, "Interval in milliseconds for generating local orders")
// 		loSize      = flag.Int("lo-size", 200, "Maximum number of transactions in one LocalOrder")
// 		txRate      = flag.Int("tx-rate", 7000, "Transaction submission rate (tx/s)")
// 		simDuration = flag.Int("sim-duration", 30, "Simulation duration in seconds")
// 	)
// 	flag.Parse()
// 	if *cpuProfile {
// 		f, _ := os.Create(fmt.Sprintf("themis_cpu_nodes_%s.pprof", strings.Replace(*nodeList, ",", "_", -1)))
// 		if f != nil {
// 			if err := pprof.StartCPUProfile(f); err != nil {
// 				log.Fatal("could not start CPU profile: ", err)
// 			}
// 			defer pprof.StopCPUProfile()
// 		}
// 	}
// 	config, totalNodesFromConfig := readJSONConfig(*configFile)
// 	nodesToRun := parseNodeList(*nodeList)
// 	if len(nodesToRun) == 0 {
// 		log.Fatal("No nodes specified. Use -nodes flag.")
// 	}
// 	fmt.Printf("--- Starting Themis Protocol Simulation ---\n")
// 	fmt.Printf("Nodes on this instance: %v\n", nodesToRun)
// 	fmt.Printf("System params: N=%d, F=%d, Gamma=%.2f\n", totalNodesFromConfig, *faultCount, *gamma)
// 	fmt.Printf("Malicious replicas are IDs >= %d\n", totalNodesFromConfig-*faultCount)
// 	fmt.Printf("Workload params: TxRate=%d/s, LO-Interval=%dms, LO-Size=%d\n", *txRate, *loInterval, *loSize)
// 	fmt.Printf("Simulation duration: %d seconds\n\n", *simDuration)
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	shutdownSignal := make(chan os.Signal, 1)
// 	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)
// 	go func() {
// 		<-shutdownSignal
// 		log.Println("\nReceived shutdown signal, initiating graceful shutdown...")
// 		cancel()
// 	}()
// 	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(*simDuration)*time.Second)
// 	defer timeoutCancel()
// 	runDistributedMode(timeoutCtx, nodesToRun, config, totalNodesFromConfig, *faultCount, *gamma, *loInterval, *loSize, *txRate)
// }
// func runDistributedMode(ctx context.Context, nodeIDs []uint64, config map[uint64]string, totalNodes, faultCount uint64, gamma float64, loIntervalMs, loSize, txRate int) {
// 	var wg sync.WaitGroup
// 	services := make(map[uint64]*ofo.OFOService)
// 	var servicesMu sync.Mutex
// 	maliciousThresholdID := totalNodes - faultCount
// 	for _, nodeID := range nodeIDs {
// 		wg.Add(1)
// 		go func(id uint64) {
// 			defer wg.Done()
// 			netConfig := network.NetworkConfig{ReplicaID: id, ReplicaAddr: config}
// 			net, err := network.NewDistributedNetwork(netConfig)
// 			if err != nil {
// 				log.Printf("Node %d failed to create network: %v", id, err)
// 				return
// 			}
// 			defer net.Stop()
// 			log.Printf("Node %d waiting for peers to connect...", id)
// 			if err := net.WaitForPeers(30 * time.Second); err != nil {
// 				log.Printf("FATAL: Node %d could not connect to all peers, shutting down. Error: %v", id, err)
// 				return
// 			}
// 			isMalicious := id >= maliciousThresholdID
// 			if isMalicious {
// 				log.Printf("Node %d is starting as a MALICIOUS replica.", id)
// 			}
// 			service := ofo.NewOFOService(id, totalNodes, faultCount, gamma, net, loSize, loIntervalMs, isMalicious)
// 			servicesMu.Lock()
// 			services[id] = service
// 			servicesMu.Unlock()
// 			service.Start(ctx)
// 			<-ctx.Done()
// 			log.Printf("Node %d shutting down...", id)
// 			service.Stop()
// 		}(nodeID)
// 	}
// 	var submittedTxCount int32
// 	var txSubmitterWg sync.WaitGroup
// 	txSubmitterWg.Add(1)
// 	go func() {
// 		defer txSubmitterWg.Done()
// 		var targetNet network.NetworkInterface
// 		for {
// 			servicesMu.Lock()
// 			if service, ok := services[LEADER_REPLICA_ID]; ok && service != nil {
// 				targetNet = service.Network()
// 			}
// 			servicesMu.Unlock()
// 			if targetNet != nil {
// 				break
// 			}
// 			select {
// 			case <-time.After(100 * time.Millisecond):
// 			case <-ctx.Done():
// 				return
// 			}
// 		}
// 		submitTransactions(ctx, targetNet, totalNodes, txRate, &submittedTxCount)
// 	}()
// 	var monitorWg sync.WaitGroup
// 	monitorWg.Add(1)
// 	go func() {
// 		defer monitorWg.Done()
// 		simDurationVal, _ := strconv.Atoi(flag.Lookup("sim-duration").Value.String())
// 		monitorSystem(ctx, services, &servicesMu, &submittedTxCount, simDurationVal)
// 	}()
// 	wg.Wait()
// 	txSubmitterWg.Wait()
// 	monitorWg.Wait()
// 	fmt.Println("\nAll nodes on this instance have shut down.")
// }
// func submitTransactions(ctx context.Context, net network.NetworkInterface, totalNodes uint64, txRate int, submittedCounter *int32) {
// 	if txRate <= 0 {
// 		return
// 	}
// 	interval := time.Second / time.Duration(txRate)
// 	if interval == 0 {
// 		interval = time.Nanosecond
// 	}
// 	ticker := time.NewTicker(interval)
// 	defer ticker.Stop()
// 	txCounter := 0
// 	log.Println("Transaction submission started.")
// 	for {
// 		select {
// 		case <-ticker.C:
// 			txCounter++
// 			atomic.AddInt32(submittedCounter, 1)
// 			txHash := sha256.Sum256([]byte(fmt.Sprintf("tx-%d-%d", txCounter, rand.Intn(1e9))))
// 			tx := &types.Transaction{ID: txHash, SubmissionTime: time.Now()}
// 			for i := uint64(0); i < totalNodes; i++ {
// 				net.Send(network.Message{Type: "Transaction", From: LEADER_REPLICA_ID, To: i, Payload: tx})
// 			}
// 		case <-ctx.Done():
// 			log.Println("Transaction submission stopping...")
// 			return
// 		}
// 	}
// }
// func monitorSystem(ctx context.Context, services map[uint64]*ofo.OFOService, servicesMu *sync.Mutex, submittedCounter *int32, simDuration int) {
// 	var leaderService *ofo.OFOService
// 	for {
// 		servicesMu.Lock()
// 		leaderService = services[LEADER_REPLICA_ID]
// 		servicesMu.Unlock()
// 		if leaderService != nil {
// 			break
// 		}
// 		select {
// 		case <-time.After(100 * time.Millisecond):
// 		case <-ctx.Done():
// 			return
// 		}
// 	}
// 	monitorTicker := time.NewTicker(time.Second)
// 	defer monitorTicker.Stop()
// 	startTime := time.Now()
// 	leaderCompletionChan := leaderService.GetLeaderCompletionChan()
// 	if leaderCompletionChan == nil {
// 		log.Fatal("Leader's completion channel is nil!")
// 	}
// 	log.Println("Monitoring started.")
// monitorLoop:
// 	for {
// 		select {
// 		case <-monitorTicker.C:
// 			finalized := leaderService.GetFinalizedCount()
// 			submitted := atomic.LoadInt32(submittedCounter)
// 			elapsed := time.Since(startTime).Seconds()
// 			tps := 0.0
// 			if elapsed > 0 {
// 				tps = float64(finalized) / elapsed
// 			}
// 			avgLatency, _ := leaderService.GetLatencyStats()
// 			poolSize := leaderService.GetTxPoolSize()
// 			lastOrderSize := leaderService.GetLastProposedOrderSize()
// 			fmt.Printf("\r[%.1fs] Finalized: %d | Submitted: %d | TPS: %.2f | Latency: %s | Pool: %d | LastOrder: %d",
// 				elapsed, finalized, submitted, tps, avgLatency.Round(time.Millisecond), poolSize, lastOrderSize)
// 		case <-ctx.Done():
// 			break monitorLoop
// 		}
// 	}
// 	<-ctx.Done()
// 	time.Sleep(100 * time.Millisecond)
// 	printFinalReport(startTime, leaderService.GetFinalizedCount(), int(atomic.LoadInt32(submittedCounter)), leaderService)
// }
// func printFinalReport(startTime time.Time, finalizedCount int, totalSubmitted int, leaderService *ofo.OFOService) {
// 	fmt.Println("\n\n--- FINAL RESULTS (Themis) ---")
// 	actualDuration := time.Since(startTime).Seconds()
// 	if actualDuration < 1.0 {
// 		actualDuration = 1.0
// 	}
// 	throughput := float64(finalizedCount) / actualDuration
// 	avgLatency, latencyCount := leaderService.GetLatencyStats()
// 	fmt.Printf("Total Submitted: %d\n", totalSubmitted)
// 	fmt.Printf("Total Finalized: %d\n", finalizedCount)
// 	fmt.Printf("Throughput: %.2f TPS\n", throughput)
// 	latencyStr := "N/A"
// 	if latencyCount > 0 {
// 		latencyStr = avgLatency.Round(time.Millisecond).String()
// 	}
// 	fmt.Printf("Average Latency: %s (from %d txs)\n", latencyStr, latencyCount)
// }

// // *** 核心修复: 添加缺失的辅助函数 ***
// func readJSONConfig(filename string) (map[uint64]string, uint64) {
// 	data, err := ioutil.ReadFile(filename)
// 	if err != nil {
// 		log.Fatalf("Failed to read config file %s: %v", filename, err)
// 	}
// 	var jsonConfig Config
// 	if err := json.Unmarshal(data, &jsonConfig); err != nil {
// 		log.Fatalf("Failed to parse config file: %v", err)
// 	}
// 	config := make(map[uint64]string)
// 	var maxID uint64 = 0
// 	for k, v := range jsonConfig.Nodes {
// 		id, _ := strconv.ParseUint(k, 10, 64)
// 		if id > maxID {
// 			maxID = id
// 		}
// 		config[id] = v
// 	}
// 	return config, maxID + 1
// }
// func parseNodeList(nodeList string) []uint64 {
// 	if nodeList == "" {
// 		return []uint64{}
// 	}
// 	parts := strings.Split(nodeList, ",")
// 	nodes := make([]uint64, 0, len(parts))
// 	for _, part := range parts {
// 		id, _ := strconv.ParseUint(strings.TrimSpace(part), 10, 64)
// 		nodes = append(nodes, id)
// 	}
// 	return nodes
// }

package main

import (
	"SpeedFair_simplify/pkg/network"
	"SpeedFair_simplify/pkg/ofo"
	"SpeedFair_simplify/pkg/types"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"flag"
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
	gob.Register(&types.LeaderProposal{})
	gob.Register(&types.ReplicaOrders{})
	gob.Register(&types.UpdateOrder{})
	gob.Register(&types.LocalOrder{})
	gob.Register(&types.LocalOrderFragment{})
	gob.Register(map[[32]byte]types.TxState{})
}

const (
	LEADER_REPLICA_ID = 0
)

type Config struct {
	Nodes map[string]string `json:"nodes"`
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var (
		configFile  = flag.String("config", "config.json", "JSON config file for node addresses")
		nodeList    = flag.String("nodes", "", "Comma-separated list of node IDs to run on this instance")
		cpuProfile  = flag.Bool("cpuprofile", false, "Enable CPU profiling for this instance")
		faultCount  = flag.Uint64("f", 1, "Number of tolerated faulty replicas")
		gamma       = flag.Float64("gamma", 0.90, "Fairness parameter gamma")
		loInterval  = flag.Int("lo-interval", 150, "Interval in milliseconds for generating local orders")
		loSize      = flag.Int("lo-size", 200, "Maximum number of transactions in one LocalOrder")
		txRate      = flag.Int("tx-rate", 7000, "Transaction submission rate (tx/s)")
		simDuration = flag.Int("sim-duration", 30, "Simulation duration in seconds")
	)
	flag.Parse()

	if *cpuProfile {
		f, _ := os.Create(fmt.Sprintf("themis_cpu_nodes_%s.pprof", strings.Replace(*nodeList, ",", "_", -1)))
		if f != nil {
			if err := pprof.StartCPUProfile(f); err != nil {
				log.Fatal("could not start CPU profile: ", err)
			}
			defer pprof.StopCPUProfile()
		}
	}

	config, totalNodesFromConfig := readJSONConfig(*configFile)
	nodesToRun := parseNodeList(*nodeList)
	if len(nodesToRun) == 0 {
		log.Fatal("No nodes specified. Use -nodes flag.")
	}

	fmt.Printf("--- Starting Themis Protocol Simulation ---\n")
	fmt.Printf("Nodes on this instance: %v\n", nodesToRun)
	fmt.Printf("System params: N=%d, F=%d, Gamma=%.2f\n", totalNodesFromConfig, *faultCount, *gamma)
	fmt.Printf("Malicious replicas are IDs >= %d\n", totalNodesFromConfig-*faultCount)
	fmt.Printf("Workload params: TxRate=%d/s, LO-Interval=%dms, LO-Size=%d\n", *txRate, *loInterval, *loSize)
	fmt.Printf("Simulation duration: %d seconds\n\n", *simDuration)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-shutdownSignal
		log.Println("\nReceived shutdown signal, initiating graceful shutdown...")
		cancel()
	}()

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(*simDuration)*time.Second)
	defer timeoutCancel()

	runDistributedMode(timeoutCtx, nodesToRun, config, totalNodesFromConfig, *faultCount, *gamma, *loInterval, *loSize, *txRate)
}


// [来源: Pkg/Main.go]
func runDistributedMode(ctx context.Context, nodeIDs []uint64, config map[uint64]string, totalNodes, faultCount uint64, gamma float64, loIntervalMs, loSize, txRate int) {
	var wg sync.WaitGroup
	services := make(map[uint64]*ofo.OFOService)
	var servicesMu sync.Mutex
	maliciousThresholdID := totalNodes - faultCount

	var submittedTxCount int32
	var txSubmitterWg sync.WaitGroup 

	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			
			// 1. 创建网络
			// [来源: Pkg/network/network.go]
			netConfig := network.NetworkConfig{ReplicaID: id, ReplicaAddr: config}
			net, err := network.NewDistributedNetwork(netConfig)
			if err != nil {
				log.Printf("Node %d failed to create network: %v", id, err)
				return
			}
			defer net.Stop()

			isMalicious := id >= maliciousThresholdID
			if isMalicious {
				log.Printf("Node %d is starting as a MALICIOUS replica.", id)
			}

			// 2. *** 关键改动: 立即创建 Service 和注册 Handler ***
			// [来源: Pkg/ofo/service.go]
			// NewOFOService 会自动调用 net.Register(id, s.handleMessage) 
			service := ofo.NewOFOService(id, totalNodes, faultCount, gamma, net, loSize, loIntervalMs, isMalicious)
			servicesMu.Lock()
			services[id] = service
			servicesMu.Unlock()

			// 3. *** 关键改动: Handler 注册后，再等待网络就绪 ***
			log.Printf("Node %d waiting for peers to connect...", id)
			if err := net.WaitForPeers(30 * time.Second); err != nil {
				log.Printf("FATAL: Node %d could not connect to all peers...: %v", id, err)
				return
			}
			log.Printf("Node %d: All peers connected!", id) // 越过栅栏


			// 4. 只有 Node 0 在网络就绪后才开始提交
			// [来源: Pkg/Main.go, Pkg/ofo/service.go]
			if id == ofo.LEADER_REPLICA_ID && txRate > 0 {
				txSubmitterWg.Add(1)
				go func() {
					defer txSubmitterWg.Done()
					submitTransactions(ctx, net, totalNodes, txRate, &submittedTxCount)
				}()
			}

			// 5. 最后，启动 Service 的主循环 (LO Ticker)
			// [来源: Pkg/ofo/service.go]
			service.Start(ctx) 
			
			<-ctx.Done()
			log.Printf("Node %d shutting down...", id)
			service.Stop()
		}(nodeID)
	}

	// [!! 删除 !!] 确保旧的 submitTransactions 协程已被删除
	/*
		var txSubmitterWg sync.WaitGroup
		...
	*/

	// 监控协程 (不变)
	var monitorWg sync.WaitGroup
	monitorWg.Add(1)
	go func() {
		defer monitorWg.Done()
		simDurationVal, _ := strconv.Atoi(flag.Lookup("sim-duration").Value.String())
		monitorSystem(ctx, services, &servicesMu, &submittedTxCount, simDurationVal)
	}()

	wg.Wait()
	txSubmitterWg.Wait() 
	monitorWg.Wait()
	fmt.Println("\nAll nodes on this instance have shut down.")
}			
			

func submitTransactions(ctx context.Context, net network.NetworkInterface, totalNodes uint64, txRate int, submittedCounter *int32) {
	if txRate <= 0 {
		return
	}
	interval := time.Second / time.Duration(txRate)
	if interval == 0 {
		interval = time.Nanosecond
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
			txHash := sha256.Sum256([]byte(fmt.Sprintf("tx-%d-%d", txCounter, rand.Intn(1e9))))
			tx := &types.Transaction{ID: txHash, SubmissionTime: time.Now()}

			for i := uint64(0); i < totalNodes; i++ {
				net.Send(network.Message{Type: "Transaction", From: 999, To: i, Payload: tx})
			}
		case <-ctx.Done():
			log.Println("Transaction submission stopping...")
			return
		}
	}
}

func monitorSystem(ctx context.Context, services map[uint64]*ofo.OFOService, servicesMu *sync.Mutex, submittedCounter *int32, simDuration int) {
	var leaderService *ofo.OFOService
	for {
		servicesMu.Lock()
		leaderService = services[LEADER_REPLICA_ID]
		servicesMu.Unlock()
		if leaderService != nil {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}

	monitorTicker := time.NewTicker(time.Second)
	defer monitorTicker.Stop()
	startTime := time.Now()

	log.Println("Monitoring started.")

monitorLoop:
	for {
		select {
		case <-monitorTicker.C:
			finalized := leaderService.GetFinalizedCount()
			submitted := atomic.LoadInt32(submittedCounter)
			elapsed := time.Since(startTime).Seconds()
			tps := 0.0
			if elapsed > 0 {
				tps = float64(finalized) / elapsed
			}
			avgLatency, _ := leaderService.GetLatencyStats()
			poolSize := leaderService.GetTxPoolSize()
			lastOrderSize := leaderService.GetLastProposedOrderSize()
			fmt.Printf("\r[%.1fs] Finalized: %d | Submitted: %d | TPS: %.2f | Latency: %s | Pool: %d | LastOrder: %d",
				elapsed, finalized, submitted, tps, avgLatency.Round(time.Millisecond), poolSize, lastOrderSize)
		case <-ctx.Done():
			break monitorLoop
		}
	}

	<-ctx.Done()
	time.Sleep(250 * time.Millisecond)
	printFinalReport(startTime, leaderService.GetFinalizedCount(), int(atomic.LoadInt32(submittedCounter)), leaderService)
}

func printFinalReport(startTime time.Time, finalizedCount int, totalSubmitted int, leaderService *ofo.OFOService) {
	fmt.Println("\n\n--- FINAL RESULTS (Themis) ---")
	actualDuration := time.Since(startTime).Seconds()
	if actualDuration < 1.0 {
		actualDuration = 1.0
	}
	throughput := float64(finalizedCount) / actualDuration
	avgLatency, latencyCount := leaderService.GetLatencyStats()

	fmt.Printf("Total Submitted: %d\n", totalSubmitted)
	fmt.Printf("Total Finalized: %d\n", finalizedCount)
	fmt.Printf("Throughput:       %.2f TPS\n", throughput)

	latencyStr := "N/A"
	if latencyCount > 0 {
		latencyStr = avgLatency.Round(time.Millisecond).String()
	}
	fmt.Printf("Average Latency:  %s (from %d txs)\n", latencyStr, latencyCount)
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
