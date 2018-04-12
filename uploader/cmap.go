package uploader

import (
	"sync"
	"sync/atomic"
	"time"
)

var shardCount = 1024

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type CMap []*CMapShard

// A "thread" safe string to anything map.
type CMapShard struct {
	sync.RWMutex // Read Write mutex, guards access to internal map.
	items        map[string]int64
}

// Creates a new concurrent map.
func NewCMap() CMap {
	m := make(CMap, shardCount)
	for i := 0; i < shardCount; i++ {
		m[i] = &CMapShard{items: make(map[string]int64)}
	}
	return m
}

// hash function
// @TODO: try crc32 or something else?
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// Returns the number of elements within the map.
func (m CMap) Count() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

func (m CMap) Clear() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		shard := m[i]
		shard.Lock()
		shard.items = make(map[string]int64)
		shard.Unlock()
	}
	return count
}

// Returns shard under given key
func (m CMap) GetShard(key string) *CMapShard {
	// @TODO: remove type casts
	return m[uint(fnv32(key))%uint(shardCount)]
}

// Retrieves an element from map under given key.
func (m CMap) Exists(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Sets the given value under the specified key.
func (m CMap) Add(key string, value int64) {
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

func (m CMap) Merge(keys map[string]bool, value int64) {
	for key, _ := range keys {
		m.Add(key, value)
	}
}

func (m CMap) Expire(exit chan struct{}, ttl time.Duration) (int, int64) {
	deadline := time.Now().Add(-ttl).Unix()

	count := 0
	min := time.Now().Unix()

	for i := 0; i < shardCount; i++ {
		select {
		case <-exit:
			return count, min
		default:
			// pass
		}

		shard := m[i]
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

func (m CMap) ExpireWorker(exit chan struct{}, ttl time.Duration, expiredCounter *uint32) {
	for {
		interval := time.Minute
		// @TODO: adaptive interval, based on min value from prev Expire run

		select {
		case <-exit:
			return
		case <-time.After(interval):
			cnt, _ := m.Expire(exit, ttl)
			if expiredCounter != nil {
				atomic.AddUint32(expiredCounter, uint32(cnt))
			}
		}
	}
}
