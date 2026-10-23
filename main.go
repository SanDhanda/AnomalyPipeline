// Anomaly detection pipeline: an in-process stand-in for a Kafka-style system.
//
//	Producer --(partition by account)--> N partition channels --> N scoring workers
//	                                                                |-> alerts channel      -> AlertService
//	                                                                '-> dead-letter channel -> DeadLetterService
//
// Concepts demonstrated (each one maps onto something you'd do with Kafka + Go):
//   - Partitioning by key: same account always lands on the same partition, so per-account ordering holds
//   - Consumer parallelism: one worker per partition (more partitions = more parallelism)
//   - Backpressure: bounded channels make the producer wait if a worker falls behind
//   - Idempotency: duplicate deliveries are detected by event ID and skipped
//   - Retries with backoff, then a dead-letter queue for events that keep failing
//   - Simple "model": per-account running mean and standard deviation, flagging large z-scores
package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	partitionCount := 4
	totalEvents := 5000

	var stats Stats
	// One bounded channel per partition. Bounded = backpressure: if a worker falls behind,
	// WriteAsync waits instead of memory growing without limit.
	partitions := make([]chan Transaction, partitionCount)
	for i := range partitions {
		partitions[i] = make(chan Transaction, 500)
	}

	var alerts = make(chan Alert)
	var deadLetters = make(chan Transaction)
	start := time.Now()

	var scorers sync.WaitGroup
	for id, ch := range partitions {
		scorers.Add(1)
		go func(id int, in <-chan Transaction) {
			defer scorers.Done()
			runScorer(id, in, alerts, deadLetters, &stats)
		}(id, ch)
	}
	var services sync.WaitGroup
	services.Add(2)
	go func() {
		defer services.Done()
		runAlert(alerts, &stats)
	}()

	go func() {
		defer services.Done()
		runDeadLetter(deadLetters, &stats)
	}()

	go func() { runProducer(partitions, totalEvents, &stats) }() // closes the partition channels when finished
	scorers.Wait()                                               // wait until every partition has drained
	close(alerts)

	close(deadLetters)
	services.Wait()

	elapsed := time.Since(start)
	seconds := elapsed.Seconds()
	var precision float64 = 0
	if stats.Alerts == 0 {
		precision = 0
	} else {
		precision = float64(stats.TruePositives) / float64(stats.Alerts)
	}

	var recall float64 = 0
	if stats.InjectedAnomalies == 0 {
		recall = 0
	} else {
		recall = float64(stats.TruePositives) / float64(stats.InjectedAnomalies)
	}

	fmt.Println()
	fmt.Printf("Partitions / workers:    %d\n", partitionCount)
	fmt.Printf("Events published:        %d (includes deliberate duplicates)\n", stats.Produced)
	fmt.Printf("Duplicates skipped:      %d\n", stats.DuplicatesSkipped)
	fmt.Printf("Processed:               %d\n", stats.Processed)
	fmt.Printf("Transient retries:       %d\n", stats.Retries)
	fmt.Printf("Dead-lettered:           %d\n", stats.DeadLettered)
	fmt.Printf("Anomalies injected:      %d\n", stats.InjectedAnomalies)
	fmt.Printf("Alerts raised:           %d (true %d, false %d)\n", stats.Alerts, stats.TruePositives, stats.FalsePositives)
	fmt.Printf("Precision / recall:      %.0f%% / %.0f%%\n", precision, recall)
	fmt.Printf("Elapsed:                 %.2f\n", seconds)
	fmt.Printf("Throughput:              %.0f  events/sec\n", float64(stats.Processed)/seconds)

}

// ---------------------------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------------------------
var ErrTransient = errors.New("transient model error")

type Transaction struct {
	EventId           string
	AccountId         string
	Amount            float64
	IsInjectedAnomaly bool // IsInjectedAnomaly is ground truth used only to evaluate the model. The scorer never reads it.
}

type Alert struct {
	tx        Transaction
	Score     float64
	Partition int
}

type Stats struct {
	Produced, InjectedAnomalies           int64 // written by the producer only
	Processed, DuplicatesSkipped, Retries int64 // written by many workers: use Interlocked
	Alerts, TruePositives, FalsePositives int64 // written by the alert service only
	DeadLettered                          int64 // written by the dead-letter service only
}

// Running mean and variance (Welford's algorithm): no need to store history.
// When defining a struct with methods, methods are separate to the struct
type RunningStats struct {
	_mean, _m2 float64
	Count      int64
}

func (r *RunningStats) Add(x float64) {
	r.Count++
	delta := x - r._mean
	r._mean += delta / float64(r.Count)
	r._m2 += delta * (x - r._mean)
}

func (r *RunningStats) StdDev() float64 {
	if r.Count < 2 {
		return 0
	}
	return math.Sqrt(r._m2 / float64(r.Count-1))
}

// ---------------------------------------------------------------------------------------------
// Partitioning
// ---------------------------------------------------------------------------------------------

// FNV-1a, 32-bit. Deterministic, and identical to Go's hash/fnv New32a.
func fnv1a32(s string) uint32 {
	var hash uint32
	hash = 2166136261
	for _, b := range []byte(s) {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return hash
}

// ---------------------------------------------------------------------------------------------
// Producer
// ---------------------------------------------------------------------------------------------

func runProducer(partitions []chan Transaction, total int, stats *Stats) {

	rng := rand.New(rand.NewSource(42))
	accounts := make([]string, 50)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("acc-%03d", i+1)
	}

	// Each account has its own "normal" spending pattern. (mean and sd)
	type profile struct{ mean, sd float64 }
	profiles := make(map[string]profile, len(accounts))
	for _, a := range accounts {
		profiles[a] = profile{
			mean: 20 + rng.Float64()*60,
			sd:   5 + rng.Float64()*10,
		}
	}

	send := func(tx Transaction) {
		partition := int(fnv1a32(tx.AccountId) % uint32(len(partitions)))
		partitions[partition] <- tx //send tx into this channel
		stats.Produced++
	}

	for i := range total {
		account := accounts[rng.Intn(len(accounts))]
		prof := profiles[account]
		amount := max(0.5, gaussian(rng, prof.mean, prof.sd))
		// After a warm-up, occasionally inject an anomaly: a spend 10x the normal level.
		anomaly := i > 500 && rng.Float64() < 0.005
		if anomaly {
			amount *= 10
			stats.InjectedAnomalies++
		}

		tx := Transaction{EventId: fmt.Sprintf("evt-%d", i), AccountId: account, Amount: math.Round(amount*100) / 100, IsInjectedAnomaly: anomaly}
		send(tx)

		// Simulate at-least-once delivery: occasionally the same event arrives twice.
		//Mimic real queue that sometimes delivers a message more than once
		if rng.Float64() < 0.01 {
			send(tx)
		}

	}
	// Signal "no more messages" so the workers can finish once drained.
	for _, partition := range partitions {
		close(partition)
	}
}

func gaussian(rng *rand.Rand, mean float64, sd float64) float64 {
	u1 := 1.0 - rng.Float64()
	u2 := rng.Float64()
	return mean + sd*math.Sqrt(-2.0*math.Log(u1))*math.Cos(2.0*math.Pi*u2)
}

// ---------------------------------------------------------------------------------------------
// Scoring worker: one per partition
// ---------------------------------------------------------------------------------------------

type ScorerConfig struct {
	MaxAttempts          int
	TransientFailureRate float64
	MinHistory           int
	ZThreshold           float64
}

func runScorer(partitionId int, in <-chan Transaction, alerts chan<- Alert, deadLetters chan<- Transaction, stats *Stats) {
	cfg := ScorerConfig{
		MaxAttempts:          3,
		TransientFailureRate: 0.10, // simulates a flaky model call
		MinHistory:           10,   // don't score an account until we have some history
		ZThreshold:           4.0,
	}

	rng := rand.New(rand.NewSource(int64(1000 + partitionId)))
	// These two structures need no locks: this partition is the only place its accounts are processed.
	// That is the main payoff of partitioning by key.
	seen := make(map[string]struct{})         // idempotency (unbounded here; use a TTL or bounded store in production)
	history := make(map[string]*RunningStats) // per-account state

	for tx := range in {
		// Idempotency: if we've already handled this event, skip it.
		if _, ok := seen[tx.EventId]; ok {
			atomic.AddInt64(&stats.DuplicatesSkipped, 1)
			continue
		}
		var score float64
		var scored bool
		// Retry with backoff.
		//Fails approx 10% of time to mimic flaky service
		for attempt := 1; attempt <= cfg.MaxAttempts && !scored; attempt++ {
			result, err := predict(tx, history, rng, cfg)
			if err == nil {
				score, scored = result, true
				continue
			}
			if errors.Is(err, ErrTransient) {
				atomic.AddInt64(&stats.Retries, 1)
				time.Sleep(time.Duration(2*attempt) * time.Millisecond)
			}
		}
		// Still failing after all attempts: park it on the dead-letter queue and move on.
		if !scored {
			deadLetters <- tx
			continue
		}
		// Mark as seen only after success, so a dead-lettered event could be replayed later.
		seen[tx.EventId] = struct{}{}
		atomic.AddInt64(&stats.Processed, 1)

		if score > cfg.ZThreshold {
			alerts <- Alert{tx: tx, Score: score, Partition: partitionId}

		} else {
			learn(history, tx) // only learn from transactions we believe are normal
		}
	}
}

// Stands in for a real model call. It can fail transiently, like a network call to a model server.
func predict(tx Transaction, history map[string]*RunningStats, rng *rand.Rand, cfg ScorerConfig) (float64, error) {

	burnCpu(1.0) // simulated inference cost, so adding partitions visibly helps throughput

	if rng.Float64() < cfg.TransientFailureRate {
		return 0, ErrTransient
	}
	s, ok := history[tx.AccountId]
	if !ok || int(s.Count) < cfg.MinHistory || s.StdDev() == 0 {
		return 0, nil
	}

	return math.Abs(tx.Amount-s._mean) / s.StdDev(), nil // z-score
}

func learn(history map[string]*RunningStats, tx Transaction) {
	s, ok := history[tx.AccountId]
	if !ok {
		s = &RunningStats{} //creates a zero valued struct of type RunningStats
		history[tx.AccountId] = s
	}
	s.Add(tx.Amount)
}
func burnCpu(milliseconds float64) {
	end := time.Now().Add(time.Duration(milliseconds * float64(time.Millisecond)))
	for time.Now().Before(end) {
	}
}

// ---------------------------------------------------------------------------------------------
// Downstream consumers
// ---------------------------------------------------------------------------------------------

// Processes alerts from the specified channel reader and updates statistics accordingly.
func runAlert(in <-chan Alert, stats *Stats) {
	shown := 0
	for alert := range in {
		stats.Alerts++
		if alert.tx.IsInjectedAnomaly {
			stats.TruePositives++

		} else {
			stats.FalsePositives++
		}
		if shown < 5 {
			fmt.Println("ALERT	partition=%d %s amount=%.2f score=%.1f\n", alert.Partition, alert.tx.AccountId, alert.tx.Amount, alert.Score)
		}
		shown++
	}

}

// Counts and prints the transactions that failed all their retries
func runDeadLetter(in <-chan Transaction, stats *Stats) {
	shown := 0
	for tx := range in {
		stats.DeadLettered++
		if shown < 3 {
			fmt.Println("DEAD	%s %s amount=%.2f\n", tx.EventId, tx.AccountId, tx.Amount)
		}
		shown++
	}
}
