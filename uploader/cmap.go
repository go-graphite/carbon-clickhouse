package uploader

import "sync"

var shardCount = 1024

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type CMap []*CMapShard

// A "thread" safe string to anything map.
type CMapShard struct {
	sync.RWMutex // Read Write mutex, guards access to internal map.
	items        map[string]bool
}

// Creates a new concurrent map.
func NewCMap() CMap {
	m := make(CMap, shardCount)
	for i := 0; i < shardCount; i++ {
		m[i] = &CMapShard{items: make(map[string]bool)}
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
		shard.items = make(map[string]bool)
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
func (m CMap) Add(key string) {
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = true
	shard.Unlock()
}
