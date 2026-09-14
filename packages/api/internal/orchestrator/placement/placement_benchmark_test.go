package placement

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

// BenchmarkConfig contains configuration for realistic benchmark scenarios
type BenchmarkConfig struct {
	NumNodes              int
	SandboxStartRate      int           // sandboxes per second
	AvgSandboxCPU         int64         // average CPU request
	AvgSandboxMemory      int64         // average memory request in MiB
	CPUVariance           float64       // variance in CPU request (0.0 to 1.0)
	MemoryVariance        float64       // variance in memory request (0.0 to 1.0)
	ActualUsageRatio      float64       // ratio of actual usage to requested (0.0 to 1.0)
	ActualUsageVariance   float64       // variance in actual usage
	SandboxDuration       time.Duration // how long sandboxes run
	DurationVariance      float64       // variance in sandbox duration
	BenchmarkDuration     time.Duration
	NodeCPUCapacity       uint32 // CPU capacity per node
	NodeMemoryCapacity    uint64 // Memory capacity per node in bytes
	SandboxCreateDuration time.Duration
}

// LiveSandbox represents a running sandbox with its resource usage
type LiveSandbox struct {
	ID               string
	NodeID           string
	RequestedCPU     int64
	RequestedMemory  int64
	ActualCPUUsage   float64
	ActualMemUsage   float64
	StartTime        time.Time
	PlannedDuration  time.Duration
	PlacementLatency time.Duration
}

// NodeSimulator defines the common behavior of simulated nodes
type NodeSimulator interface {
	// GetNode returns the underlying nodemanager.Node object for use by the algorithm
	GetNode() *nodemanager.Node
	// PlaceSandbox attempts to place a Sandbox on the node, returns whether successful
	PlaceSandbox(sbx *LiveSandbox) bool
	// RemoveSandbox removes a Sandbox from the node
	RemoveSandbox(sandboxID string)
	// GetUtilization returns the current CPU and memory utilization (0-100)
	GetUtilization() (float64, float64)
	// GetSandboxCount returns the number of currently running Sandboxes
	GetSandboxCount() int
}

// NodeFactory defines the function signature for creating simulated nodes
type NodeFactory func(id string, config BenchmarkConfig) NodeSimulator

// Simulated nodes back sandbox memory with 2 MiB hugepages, as the fleet does.
const benchHugePageBytes = 2 * 1024 * 1024

// hugepagePool renders a node's memory as the kernel's hugepage counters: the
// whole capacity is the pool, faulted-in bytes are used pages, committed but
// untouched bytes are reserved pages.
func hugepagePool(capacityBytes, allocatedBytes, usedBytes uint64) (total, used, reserved uint64) {
	total = capacityBytes / benchHugePageBytes
	used = usedBytes / benchHugePageBytes
	if allocatedBytes > usedBytes {
		reserved = (allocatedBytes - usedBytes) / benchHugePageBytes
	}

	return total, used, reserved
}

// StandardNode implements the original SimulatedNode logic (real-time metric synchronization)
type StandardNode struct {
	*nodemanager.Node

	memCapacity uint64 // hugepage pool in bytes; 0 reports no pool
	mu          sync.RWMutex
	sandboxes   map[string]*LiveSandbox

	// The node's real state, published by report(). Never read back from the
	// Node metrics: the algorithm's OptimisticAdd has already bumped those,
	// and a heartbeat overwrites that bump rather than stacking on it.
	cpuAllocated uint32
	cpuUsed      float64 // cores
	memAllocated uint64
	memUsed      uint64

	totalPlacements    atomic.Int64
	rejectedPlacements atomic.Int64 // counts failed placements due to capacity
}

// Ensure StandardNode implements the interface
var _ NodeSimulator = &StandardNode{}

func NewStandardNode(id string, config BenchmarkConfig) NodeSimulator {
	n := &StandardNode{
		Node: nodemanager.NewTestNode(
			id,
			api.NodeStatusReady,
			0,
			config.NodeCPUCapacity,
			nodemanager.WithSandboxSleepingClient(config.SandboxCreateDuration),
		),
		memCapacity: config.NodeMemoryCapacity,
		sandboxes:   make(map[string]*LiveSandbox),
	}
	n.report()

	return n
}

func (n *StandardNode) GetNode() *nodemanager.Node {
	return n.Node
}

// report publishes the real state the way an orchestrator heartbeat does.
func (n *StandardNode) report() {
	total, used, reserved := hugepagePool(n.memCapacity, n.memAllocated, n.memUsed)

	n.UpdateMetricsFromServiceInfoResponse(&orchestrator.ServiceInfoResponse{
		MetricSandboxesRunning:     uint32(len(n.sandboxes)),
		MetricCpuPercent:           uint32(n.cpuUsed * 100),
		MetricCpuAllocated:         n.cpuAllocated,
		MetricCpuCount:             n.Metrics().CpuCount,
		MetricMemoryUsedBytes:      n.memUsed,
		MetricMemoryAllocatedBytes: n.memAllocated,
		MetricMemoryTotalBytes:     n.memCapacity,
		MetricHugepagesTotal:       total,
		MetricHugepagesUsed:        used,
		MetricHugepagesReserved:    reserved,
		MetricHugepageSizeBytes:    benchHugePageBytes,
	})
}

// admit takes the sandbox into the real state, or refuses it. CPU is
// over-committed; the hugepage pool is physical and refuses past its size.
func (n *StandardNode) admit(sbx *LiveSandbox) bool {
	requestedMem := uint64(sbx.RequestedMemory) * 1024 * 1024

	if n.cpuAllocated+uint32(sbx.RequestedCPU) > n.Metrics().CpuCount*4 ||
		(n.memCapacity > 0 && n.memAllocated+requestedMem > n.memCapacity) {
		n.rejectedPlacements.Add(1)

		return false
	}

	n.sandboxes[sbx.ID] = sbx
	n.cpuAllocated += uint32(sbx.RequestedCPU)
	n.cpuUsed += sbx.ActualCPUUsage
	n.memAllocated += requestedMem
	n.memUsed += uint64(sbx.ActualMemUsage)
	n.totalPlacements.Add(1)

	return true
}

// release drops the sandbox from the real state.
func (n *StandardNode) release(sandboxID string) {
	sbx, exists := n.sandboxes[sandboxID]
	if !exists {
		return
	}

	delete(n.sandboxes, sandboxID)
	n.cpuAllocated -= uint32(sbx.RequestedCPU)
	n.cpuUsed -= sbx.ActualCPUUsage
	n.memAllocated -= uint64(sbx.RequestedMemory) * 1024 * 1024
	n.memUsed -= uint64(sbx.ActualMemUsage)
}

func (n *StandardNode) PlaceSandbox(sbx *LiveSandbox) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.admit(sbx) {
		return false
	}

	n.report()

	return true
}

func (n *StandardNode) RemoveSandbox(sandboxID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.release(sandboxID)
	n.report()
}

func (n *StandardNode) GetUtilization() (float64, float64) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	metrics := n.Metrics()
	var cpuUtil, memUtil float64
	if metrics.CpuCount > 0 {
		cpuUtil = ((float64(metrics.CpuPercent) / 100) / float64(metrics.CpuCount)) * 100
	}
	if metrics.MemoryTotalBytes > 0 {
		memUtil = (float64(metrics.MemoryUsedBytes) / float64(metrics.MemoryTotalBytes)) * 100
	}

	return cpuUtil, memUtil
}

func (n *StandardNode) GetSandboxCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.sandboxes)
}

// LaggyNode simulates metric lag.
// It maintains both the “real” state and “reported” state (Node Metrics).
// Real state is only synced to Node Metrics when SyncMetrics() is called.
type LaggyNode struct {
	*StandardNode // Embed StandardNode to reuse logic
}

func NewLaggyNode(id string, config BenchmarkConfig) NodeSimulator {
	base := NewStandardNode(id, config).(*StandardNode)

	return &LaggyNode{
		StandardNode: base,
	}
}

// PlaceSandbox Override: only update real state, not Node Metrics
func (n *LaggyNode) PlaceSandbox(sbx *LiveSandbox) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Key: intentionally do NOT report.
	// The metrics visible to Orchestrator remain unchanged until SyncMetrics is called
	return n.admit(sbx)
}

func (n *LaggyNode) RemoveSandbox(sandboxID string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.release(sandboxID)
}

// SyncMetrics simulates heartbeat reporting, syncing real state to Orchestrator
func (n *LaggyNode) SyncMetrics() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.report()
}

// BenchmarkMetrics contains detailed metrics from the benchmark
type BenchmarkMetrics struct {
	// Placement metrics
	TotalPlacements      int64
	SuccessfulPlacements int64
	FailedPlacements     int64
	AvgPlacementTime     time.Duration
	MaxPlacementTime     time.Duration
	MinPlacementTime     time.Duration
	P50PlacementTime     time.Duration
	P95PlacementTime     time.Duration
	P99PlacementTime     time.Duration

	// Node utilization metrics
	AvgNodeCPUUtilization float64
	MaxNodeCPUUtilization float64
	MinNodeCPUUtilization float64
	AvgNodeMemUtilization float64
	MaxNodeMemUtilization float64
	MinNodeMemUtilization float64

	// Load distribution metrics
	CPULoadStdDev            float64
	MemLoadStdDev            float64
	LoadImbalanceCoefficient float64
}

// createSimulatedNodes creates nodes using a factory function
func createSimulatedNodes(config BenchmarkConfig, factory NodeFactory) []NodeSimulator {
	if factory == nil {
		factory = NewStandardNode // Default to standard node
	}

	nodes := make([]NodeSimulator, config.NumNodes)
	for i := range config.NumNodes {
		nodes[i] = factory(fmt.Sprintf("node-%d", i), config)
	}

	return nodes
}

// Helper function: converts NodeSimulator slice to *nodemanager.Node slice (for algorithm use)
func toNodeManagerNodes(simNodes []NodeSimulator) []*nodemanager.Node {
	nodes := make([]*nodemanager.Node, len(simNodes))
	for i, n := range simNodes {
		nodes[i] = n.GetNode()
	}

	return nodes
}

// runBenchmark runs a comprehensive placement benchmark with lifecycle tracking
func runBenchmark(b *testing.B, algorithm Algorithm, config BenchmarkConfig, nodeFactory NodeFactory) *BenchmarkMetrics {
	b.Helper()

	ctx, cancel := context.WithTimeout(b.Context(), config.BenchmarkDuration)
	defer cancel()

	// Create nodes using factory
	simNodes := createSimulatedNodes(config, nodeFactory)

	// Convert node list
	nodes := toNodeManagerNodes(simNodes)
	nodeMap := make(map[string]NodeSimulator)
	for _, n := range simNodes {
		nodeMap[n.GetNode().ID] = n
	}

	// Initialize metrics
	metrics := &BenchmarkMetrics{
		MinPlacementTime: time.Hour,
	}
	// Tracking structures
	var (
		mu               sync.Mutex
		placementTimes   []time.Duration
		activeSandboxes  sync.Map // sandboxID -> *LiveSandbox
		sandboxIDCounter atomic.Int64

		// Metrics for time series
		recentPlacements []time.Duration
		recentSuccesses  atomic.Int64
		recentFailures   atomic.Int64
	)

	// Start sandbox cleanup goroutine
	stopCleanup := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				// Check and remove expired sandboxes
				activeSandboxes.Range(func(key, value any) bool {
					sandbox := value.(*LiveSandbox)
					if now.Sub(sandbox.StartTime) > sandbox.PlannedDuration {
						// Remove from node
						if node, exists := nodeMap[sandbox.NodeID]; exists {
							node.RemoveSandbox(sandbox.ID)
						}
						// Remove from active list
						activeSandboxes.Delete(key)
					}

					return true
				})
			case <-stopCleanup:
				return
			}
		}
	}()

	// Sandbox generation goroutine
	var wg sync.WaitGroup
	stopGeneration := make(chan struct{})

	wg.Go(func() {
		ticker := time.NewTicker(time.Second / time.Duration(config.SandboxStartRate))
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Generate sandbox with variance
				sandboxID := sandboxIDCounter.Add(1)

				cpuVariance := (rand.Float64()*2 - 1) * config.CPUVariance
				requestedCPU := max(int64(float64(config.AvgSandboxCPU)*(1+cpuVariance)), 1)

				memVariance := (rand.Float64()*2 - 1) * config.MemoryVariance
				requestedMem := max(int64(float64(config.AvgSandboxMemory)*(1+memVariance)), 1)

				// Calculate actual usage with variance
				actualUsageRatio := config.ActualUsageRatio + (rand.Float64()*2-1)*config.ActualUsageVariance
				if actualUsageRatio < 0.1 {
					actualUsageRatio = 0.1
				}
				if actualUsageRatio > 1.0 {
					actualUsageRatio = 1.0
				}

				// Calculate duration with variance
				durationVariance := (rand.Float64()*2 - 1) * config.DurationVariance
				duration := time.Duration(float64(config.SandboxDuration) * (1 + durationVariance))

				sandbox := &LiveSandbox{
					ID:              fmt.Sprintf("sandbox-%d", sandboxID),
					RequestedCPU:    requestedCPU,
					RequestedMemory: requestedMem,
					ActualCPUUsage:  float64(requestedCPU) * actualUsageRatio,
					ActualMemUsage:  float64(requestedMem) * actualUsageRatio * 1024 * 1024, // Convert MiB to bytes
					StartTime:       time.Now(),
					PlannedDuration: duration,
				}

				// Try to place the sandbox
				wg.Go(func(sbx *LiveSandbox) func() {
					return func() {
						placementStart := time.Now()
						result, err := PlaceSandbox(ctx, algorithm, nodes, nil, &orchestratorgrpc.SandboxCreateRequest{Sandbox: &orchestratorgrpc.SandboxConfig{
							SandboxId: sbx.ID,
							Vcpu:      sbx.RequestedCPU,
							RamMb:     sbx.RequestedMemory,
						}}, CPURequirement{}, false, nil)

						placementTime := time.Since(placementStart)
						sbx.PlacementLatency = placementTime

						mu.Lock()
						placementTimes = append(placementTimes, placementTime)
						recentPlacements = append(recentPlacements, placementTime)
						metrics.TotalPlacements++

						if placementTime < metrics.MinPlacementTime {
							metrics.MinPlacementTime = placementTime
						}
						if placementTime > metrics.MaxPlacementTime {
							metrics.MaxPlacementTime = placementTime
						}

						success := false
						if err == nil && result.Node != nil {
							// Find the simulated node and place the sandbox
							if simNode, exists := nodeMap[result.Node.ID]; exists {
								sbx.NodeID = result.Node.ID
								if simNode.PlaceSandbox(sbx) {
									activeSandboxes.Store(sbx.ID, sbx)
									metrics.SuccessfulPlacements++
									recentSuccesses.Add(1)
									success = true
								}
							}
						}

						if !success {
							metrics.FailedPlacements++
							recentFailures.Add(1)
						}
						mu.Unlock()
					}
				}(sandbox))

			case <-stopGeneration:
				return
			case <-ctx.Done():
				return
			}
		}
	})

	// Wait for benchmark duration
	<-ctx.Done()
	close(stopGeneration)
	wg.Wait()
	close(stopCleanup)

	// Calculate final metrics
	calculateFinalMetrics(metrics, simNodes, placementTimes)

	return metrics
}

// calculateFinalMetrics calculates comprehensive final metrics
func calculateFinalMetrics(metrics *BenchmarkMetrics, nodes []NodeSimulator, placementTimes []time.Duration) {
	// Calculate placement time metrics
	if len(placementTimes) > 0 {
		var totalTime time.Duration
		for _, t := range placementTimes {
			totalTime += t
		}
		metrics.AvgPlacementTime = totalTime / time.Duration(len(placementTimes))

		// Sort for percentiles
		slices.Sort(placementTimes)

		metrics.P50PlacementTime = placementTimes[len(placementTimes)*50/100]
		metrics.P95PlacementTime = placementTimes[len(placementTimes)*95/100]
		if len(placementTimes) > 0 {
			metrics.P99PlacementTime = placementTimes[len(placementTimes)*99/100]
		}
	}

	// Calculate node utilization metrics
	var cpuUtils, memUtils []float64
	metrics.MinNodeCPUUtilization = 100.0
	metrics.MinNodeMemUtilization = 100.0

	for _, node := range nodes {
		cpuUtil, memUtil := node.GetUtilization()
		cpuUtils = append(cpuUtils, cpuUtil)
		memUtils = append(memUtils, memUtil)

		// Track min/max
		if cpuUtil > metrics.MaxNodeCPUUtilization {
			metrics.MaxNodeCPUUtilization = cpuUtil
		}
		if cpuUtil < metrics.MinNodeCPUUtilization {
			metrics.MinNodeCPUUtilization = cpuUtil
		}
		if memUtil > metrics.MaxNodeMemUtilization {
			metrics.MaxNodeMemUtilization = memUtil
		}
		if memUtil < metrics.MinNodeMemUtilization {
			metrics.MinNodeMemUtilization = memUtil
		}
	}

	// Calculate averages and standard deviations
	if len(nodes) > 0 {
		var totalCPU, totalMem float64
		for i := range cpuUtils {
			totalCPU += cpuUtils[i]
			totalMem += memUtils[i]
		}
		metrics.AvgNodeCPUUtilization = totalCPU / float64(len(nodes))
		metrics.AvgNodeMemUtilization = totalMem / float64(len(nodes))

		// Calculate standard deviations
		var cpuSumSquares, memSumSquares float64
		for i := range cpuUtils {
			cpuDiff := cpuUtils[i] - metrics.AvgNodeCPUUtilization
			memDiff := memUtils[i] - metrics.AvgNodeMemUtilization
			cpuSumSquares += cpuDiff * cpuDiff
			memSumSquares += memDiff * memDiff
		}
		metrics.CPULoadStdDev = math.Sqrt(cpuSumSquares / float64(len(nodes)))
		metrics.MemLoadStdDev = math.Sqrt(memSumSquares / float64(len(nodes)))

		// Calculate coefficient of variation (load imbalance)
		if metrics.AvgNodeCPUUtilization > 0 {
			metrics.LoadImbalanceCoefficient = metrics.CPULoadStdDev / metrics.AvgNodeCPUUtilization
		}
	}
}

// BenchmarkPlacementComparison runs comprehensive comparison with lifecycle tracking
// Run command: go test -v -bench=BenchmarkPlacementComparison -run=^$ -benchtime=1x ./internal/orchestrator/placement
func BenchmarkPlacementComparison(t *testing.B) {
	cpuBound := BenchmarkConfig{
		NumNodes:              50,
		SandboxStartRate:      30,
		AvgSandboxCPU:         4,
		AvgSandboxMemory:      1024,
		CPUVariance:           0.8,
		MemoryVariance:        0.4,
		ActualUsageRatio:      0.4,
		ActualUsageVariance:   1,
		SandboxDuration:       5 * time.Second,
		DurationVariance:      5,
		BenchmarkDuration:     time.Minute,
		NodeCPUCapacity:       32,
		NodeMemoryCapacity:    128 * 1024 * 1024 * 1024,
		SandboxCreateDuration: time.Millisecond * 0,
	}

	// vCPU and RAM vary independently, so sandboxes equal in vCPU span 1 GiB
	// to 15 GiB, and the pool fills at ~2/3 average while CPU stays near idle.
	memorySkewed := cpuBound
	memorySkewed.SandboxStartRate = 40
	memorySkewed.AvgSandboxCPU = 2
	memorySkewed.AvgSandboxMemory = 8 * 1024
	memorySkewed.CPUVariance = 0.5
	memorySkewed.MemoryVariance = 0.9
	memorySkewed.DurationVariance = 0.5
	memorySkewed.NodeMemoryCapacity = 48 * 1024 * 1024 * 1024

	scenarios := []struct {
		name   string
		config BenchmarkConfig
	}{
		{"cpu-bound", cpuBound},
		{"memory-skewed", memorySkewed},
	}

	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"BestOfK_K3", NewBestOfK(DefaultBestOfKConfig())},
		{"BestOfK_K5", NewBestOfK(BestOfKConfig{R: 4, K: 5, Alpha: 0.5})},
	}

	for _, scenario := range scenarios {
		for _, alg := range algorithms {
			t.Run(scenario.name+"/"+alg.name, func(t *testing.B) {
				metrics := runBenchmark(t, alg.algo, scenario.config, NewStandardNode)

				t.Logf("\n=== %s / %s Results ===", scenario.name, alg.name)
				t.Logf("Placement Performance:")
				t.Logf("  Total: %d, Success: %d (%.1f%%), Failed: %d",
					metrics.TotalPlacements,
					metrics.SuccessfulPlacements,
					float64(metrics.SuccessfulPlacements)/float64(metrics.TotalPlacements)*100,
					metrics.FailedPlacements)
				t.Logf("  Latency - Avg: %v, P50: %v, P95: %v, P99: %v",
					metrics.AvgPlacementTime,
					metrics.P50PlacementTime,
					metrics.P95PlacementTime,
					metrics.P99PlacementTime)

				t.Logf("\nNode Utilization:")
				t.Logf("  CPU - Avg: %.1f%%, Min: %.1f%%, Max: %.1f%%, StdDev: %.1f%%",
					metrics.AvgNodeCPUUtilization,
					metrics.MinNodeCPUUtilization,
					metrics.MaxNodeCPUUtilization,
					metrics.CPULoadStdDev)
				t.Logf("  Memory - Avg: %.1f%%, Min: %.1f%%, Max: %.1f%%, StdDev: %.1f%%",
					metrics.AvgNodeMemUtilization,
					metrics.MinNodeMemUtilization,
					metrics.MaxNodeMemUtilization,
					metrics.MemLoadStdDev)
				t.Logf("  Load Imbalance Coefficient: %.3f", metrics.LoadImbalanceCoefficient)
			})
		}
	}
}

// BenchmarkPlacementDistribution visualizes load distribution across nodes (simulating metric lag)
// Run command: go test -v -bench=BenchmarkPlacementDistribution -run=^$ ./internal/orchestrator/placement
func BenchmarkPlacementDistribution(b *testing.B) {
	// Scenario config: “thundering herd” test under extremely high concurrency
	// Use LaggyNode to simulate metric reporting delay: Orchestrator always sees stale metrics
	config := BenchmarkConfig{
		NumNodes:              10,   // Fewer nodes to make load hotspots more apparent
		SandboxStartRate:      20,   // 20 requests per second (burst traffic)
		AvgSandboxCPU:         1,    // 1 vCPU per Sandbox
		AvgSandboxMemory:      1024, // 1024 MiB
		CPUVariance:           0.0,  // Fixed spec for easier distribution observation
		MemoryVariance:        0.0,
		ActualUsageRatio:      1.0, // Assume full utilization
		ActualUsageVariance:   0.0,
		SandboxDuration:       1 * time.Minute,          // Long enough to ensure no release during test
		BenchmarkDuration:     25 * time.Second,         // Run long enough to trigger at least one heartbeat sync (20s ticker)
		NodeCPUCapacity:       192,                      // 192 vCPU per Node
		NodeMemoryCapacity:    1024 * 1024 * 1024 * 512, // 512 GiB per Node
		SandboxCreateDuration: time.Millisecond,
	}

	algorithms := []struct {
		name string
		algo Algorithm
	}{
		// Compare algorithms here. Expect LeastBusy to have serious hotspot issues.
		// {"LeastBusy", &LeastBusyAlgorithm{}},
		{"BestOfK_K3", NewBestOfK(DefaultBestOfKConfig())},
		{"BestOfK_K5", NewBestOfK(BestOfKConfig{R: 4, K: 5, Alpha: 0.5})},
	}

	for _, alg := range algorithms {
		b.Run(alg.name, func(b *testing.B) {
			b.Logf("Running distribution test for %s with LaggyNodes...", alg.name)

			ctx, cancel := context.WithTimeout(b.Context(), config.BenchmarkDuration)
			defer cancel()

			// 1. Create nodes using NewLaggyNode (default: only update internal real state, not Metrics)
			simNodes := createSimulatedNodes(config, NewLaggyNode)
			nodes := toNodeManagerNodes(simNodes)
			nodeMap := make(map[string]NodeSimulator)

			// 2. Warmup: apply slight random baseline noise to nodes
			// This breaks the initial tie in LeastBusy state, causing it to quickly lock onto a “victim”
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for _, n := range simNodes {
				nodeMap[n.GetNode().ID] = n
				if ln, ok := n.(*LaggyNode); ok {
					ln.cpuAllocated = uint32(rng.Intn(5)) // 0-4 CPU random baseline noise
					ln.SyncMetrics()                      // Initial sync once
				}
			}

			// 3. Start heartbeat simulator
			// Simulates reporting metrics every 20s in a real environment.
			// All requests between heartbeats see stale metrics.
			go func() {
				ticker := time.NewTicker(20 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						for _, n := range simNodes {
							if ln, ok := n.(*LaggyNode); ok {
								ln.SyncMetrics()
							}
						}
					}
				}
			}()

			// 4. Concurrently generate Sandbox requests
			var wg sync.WaitGroup
			wg.Go(func() {
				ticker := time.NewTicker(time.Second / time.Duration(config.SandboxStartRate))
				defer ticker.Stop()

				var sandboxIDCounter atomic.Int64
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						sandboxID := sandboxIDCounter.Add(1)
						sbx := &LiveSandbox{
							ID:              fmt.Sprintf("sbx-%d", sandboxID),
							RequestedCPU:    config.AvgSandboxCPU,
							RequestedMemory: config.AvgSandboxMemory,
							ActualCPUUsage:  float64(config.AvgSandboxCPU),
							ActualMemUsage:  float64(config.AvgSandboxMemory) * 1024 * 1024,
							StartTime:       time.Now(),
						}

						wg.Add(1)
						go func(s *LiveSandbox) {
							// Execute placement algorithm
							result, err := PlaceSandbox(ctx, alg.algo, nodes, nil, &orchestratorgrpc.SandboxCreateRequest{
								Sandbox: &orchestratorgrpc.SandboxConfig{
									SandboxId: s.ID,
									Vcpu:      s.RequestedCPU,
									RamMb:     s.RequestedMemory,
								},
							}, CPURequirement{}, false, nil)

							if err == nil && result.Node != nil {
								if simNode, ok := nodeMap[result.Node.ID]; ok {
									// Placement successful, but Metrics won't update immediately (LaggyNode feature)
									simNode.PlaceSandbox(s)
								}
							}
						}(sbx)
					}
				}
			})

			<-ctx.Done()
			wg.Wait()

			// 5. Result visualization
			b.Logf("\n=== Distribution Results (Laggy Metrics): %s ===", alg.name)

			maxCount := 0
			totalCount := 0
			for _, n := range simNodes {
				count := n.GetSandboxCount()
				totalCount += count
				if count > maxCount {
					maxCount = count
				}
			}

			if totalCount == 0 {
				b.Log("No sandboxes placed.")

				return
			}

			// Print ASCII histogram
			for i, n := range simNodes {
				count := n.GetSandboxCount()
				barLen := 0
				if maxCount > 0 {
					barLen = int(float64(count) / float64(maxCount) * 40) // Max 40 characters
				}

				var bar strings.Builder
				for range barLen {
					bar.WriteString("█")
				}

				// Force Sync before printing to ensure we see the final real values
				if ln, ok := n.(*LaggyNode); ok {
					ln.SyncMetrics()
				}
				cpuUtil, _ := n.GetUtilization()

				b.Logf("Node %02d |%s| %d (CPU: %.1f%%)", i, fmt.Sprintf("%-40s", bar.String()), count, cpuUtil)
			}

			// Calculate distribution statistics (CV coefficient of variation)
			avg := float64(totalCount) / float64(len(simNodes))
			var sumDiffSq float64
			for _, n := range simNodes {
				diff := float64(n.GetSandboxCount()) - avg
				sumDiffSq += diff * diff
			}
			stdDev := math.Sqrt(sumDiffSq / float64(len(simNodes)))
			cv := 0.0
			if avg > 0 {
				cv = stdDev / avg
			}
			b.Logf("Stats: Total=%d, Avg=%.1f, StdDev=%.2f, Imbalance(CV)=%.3f", totalCount, avg, stdDev, cv)
		})
	}
}
