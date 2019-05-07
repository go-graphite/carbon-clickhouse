package uploader

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash"
)

const shardCount = 1024

// A "thread" safe map of type string:int64
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type CMap struct {
	shards []*CMapShard
}

// A "thread" safe string to anything map.
type CMapShard struct {
	sync.RWMutex // Read Write mutex, guards access to internal map.
	items        map[string]int64
}

// Creates a new concurrent map.
func NewCMap() *CMap {
	m := &CMap{
		shards: make([]*CMapShard, shardCount),
	}

	for i := 0; i < shardCount; i++ {
		m.shards[i] = &CMapShard{items: make(map[string]int64)}
	}

	return m
}

// Returns the number of elements within the map.
func (m CMap) Count() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

func (m CMap) Clear() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		shard := m.shards[i]
		shard.Lock()
		shard.items = make(map[string]int64)
		shard.Unlock()
	}
	return count
}

// Returns shard under given key
func (m *CMap) GetShard(key string) *CMapShard {
	// @TODO: remove type casts
	return m.shards[xxhash.Sum64String(key)%shardCount]
}

// Retrieves an element from map under given key.
func (m *CMap) Exists(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Sets the given value under the specified key.
func (m *CMap) Add(key string, value int64) {
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

func (m *CMap) Merge(keys map[string]bool, value int64) {
	for key, _ := range keys {
		m.Add(key, value)
	}
}

func (m *CMap) Expire(ctx context.Context, ttl time.Duration) (int, int64) {
	deadline := time.Now().Add(-ttl).Unix()

	count := 0
	min := time.Now().Unix()

	for i := 0; i < shardCount; i++ {
		select {
		case <-ctx.Done():
			return count, min
		default:
			// pass
		}

		shard := m.shards[i]
		shard.Lock()
		for k, v := range shard.items {
			if v < deadline {
				delete(shard.items, k)
				count++
			} else if v < min {
				min = v
			}
		}
		shard.Unlock()
	}
	return count, min
}

func (m *CMap) ExpireWorker(ctx context.Context, ttl time.Duration, expiredCounter *uint32) {
	for {
		interval := time.Minute
		// @TODO: adaptive interval, based on min value from prev Expire run

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			cnt, _ := m.Expire(ctx, ttl)
			if expiredCounter != nil {
				atomic.AddUint32(expiredCounter, uint32(cnt))
			}
		}
	}
}
