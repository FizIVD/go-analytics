package generator

import (
	"bytes"
	"compress/gzip"
	"container/heap"
	"context"
	"hash/fnv"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
)

// Event is the generator's internal representation of an event, including scheduling time.
type Event struct {
	DeviceID  string                 `json:"device_id"`
	ProfileID int64                  `json:"profile_id"`
	Action    string                 `json:"action"`
	Extras    map[string]interface{} `json:"extras"`
	Time      time.Time              `json:"-"`
}

// EventQueue is a priority queue for events.
type EventQueue []*Event

func (pq EventQueue) Len() int           { return len(pq) }
func (pq EventQueue) Less(i, j int) bool { return pq[i].Time.Before(pq[j].Time) }
func (pq EventQueue) Swap(i, j int)      { pq[i], pq[j] = pq[j], pq[i] }
func (pq *EventQueue) Push(x interface{}) { *pq = append(*pq, x.(*Event)) }
func (pq *EventQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// ShardedEventGenerator manages event scheduling across multiple shards.
type ShardedEventGenerator struct {
	shards      []*EventShard
	numShards   int
	client      *http.Client
	endpoint    string
	totalEvents *atomic.Int64
}

// EventShard is a single shard in the generator.
type EventShard struct {
	id    int
	queue EventQueue
	mu    sync.Mutex
	cond  *sync.Cond
	wg    sync.WaitGroup
}

// NewShardedEventGenerator creates a new sharded event generator.
func NewShardedEventGenerator(numShards int, endpoint string, totalEvents *atomic.Int64) *ShardedEventGenerator {
	shards := make([]*EventShard, numShards)
	for i := 0; i < numShards; i++ {
		s := &EventShard{id: i, queue: make(EventQueue, 0)}
		heap.Init(&s.queue)
		s.cond = sync.NewCond(&s.mu)
		shards[i] = s
	}
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: transport,
	}
	return &ShardedEventGenerator{
		shards:      shards,
		numShards:   numShards,
		client:      client,
		endpoint:    endpoint,
		totalEvents: totalEvents,
	}
}

func (seg *ShardedEventGenerator) getShard(userID string) *EventShard {
	h := fnv.New32a()
	h.Write([]byte(userID))
	return seg.shards[h.Sum32()%uint32(seg.numShards)]
}

// Schedule adds an event to the appropriate shard's queue.
func (seg *ShardedEventGenerator) Schedule(ev *Event) {
	shard := seg.getShard(ev.DeviceID)
	shard.mu.Lock()
	heap.Push(&shard.queue, ev)
	shard.cond.Signal()
	shard.mu.Unlock()
}

// Start launches the worker for each shard.
func (seg *ShardedEventGenerator) Start(ctx context.Context) {
	for _, shard := range seg.shards {
		shard.wg.Add(1)
		go seg.runShard(ctx, shard)
	}
}

func (seg *ShardedEventGenerator) runShard(ctx context.Context, shard *EventShard) {
	defer shard.wg.Done()
	for {
		shard.mu.Lock()
		for shard.queue.Len() == 0 {
			if ctx.Err() != nil {
				shard.mu.Unlock()
				return
			}
			shard.cond.Wait()
			if ctx.Err() != nil && shard.queue.Len() == 0 {
				shard.mu.Unlock()
				return
			}
		}
		ev := heap.Pop(&shard.queue).(*Event)
		shard.mu.Unlock()

		now := time.Now()
		if ev.Time.After(now) {
			select {
			case <-time.After(ev.Time.Sub(now)):
			case <-ctx.Done():
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		seg.processEvent(ev)
	}
}

// ClearShards forcefully clears all event queues.
func (seg *ShardedEventGenerator) ClearShards() {
	for _, shard := range seg.shards {
		shard.mu.Lock()
		shard.queue = make(EventQueue, 0)
		shard.cond.Broadcast()
		shard.mu.Unlock()
	}
}

// Wait waits for all shard workers to finish.
func (seg *ShardedEventGenerator) Wait() {
	for _, shard := range seg.shards {
		shard.wg.Wait()
	}
}

func (seg *ShardedEventGenerator) processEvent(ev *Event) {
	data, _ := sonic.ConfigFastest.Marshal(ev)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(data)
	zw.Close()

	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(reqCtx, http.MethodPost, seg.endpoint, &buf)
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")
	_, err := seg.client.Do(req)
	if err != nil {
		log.Printf("send error: %v", err)
	}

	seg.totalEvents.Add(1)
}
