// Package memcached implements a key-value [backend.Cache] client interface to a vanilla memcached implementation.
package memcached

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/bradfitz/gomemcache/memcache"
)

// A memcached client wrapper that implements the [backend.Cache] interface
type Memcached struct {
	backend.Cache
	Client *memcache.Client
}

// Instantiates a new memcached client to a memcached instance running at `serverAddress`
func NewMemcachedClient(ctx context.Context, serverAddress string) (*Memcached, error) {
	cache := &Memcached{}
	client := memcache.New(serverAddress)
	client.MaxIdleConns = 1000
	cache.Client = client
	return cache, nil
}

// Implements the backend.Cache interface
func (m *Memcached) Put(ctx context.Context, key string, value interface{}) error {
	marshaled_val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return m.Client.Set(&memcache.Item{Key: key, Value: marshaled_val})
}

// Implements the backend.Cache interface
func (m *Memcached) Get(ctx context.Context, key string, value interface{}) (bool, error) {
	it, err := m.Client.Get(key)
	if err == memcache.ErrCacheMiss {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(it.Value, value)
}

// Implements the backend.Cache interface
func (m *Memcached) Incr(ctx context.Context, key string) (int64, error) {
	val, err := m.Client.Increment(key, 1)
	return int64(val), err
}

// Implements the backend.Cache interface
func (m *Memcached) Delete(ctx context.Context, key string) error {
	return m.Client.Delete(key)
}

// Implements the backend.Cache interface
func (m *Memcached) Mget(ctx context.Context, keys []string, values []interface{}) error {
	val_map, err := m.Client.GetMulti(keys)
	if err != nil {
		return err
	}
	for idx, key := range keys {
		if val, ok := val_map[key]; ok {
			err := json.Unmarshal(val.Value, values[idx])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Implements the backend.Cache interface
func (m *Memcached) Mset(ctx context.Context, keys []string, values []interface{}) error {
	var wg sync.WaitGroup
	wg.Add(len(keys))
	err_chan := make(chan error, len(keys))
	for idx, key := range keys {
		go func(key string, val interface{}) {
			defer wg.Done()
			err_chan <- m.Put(ctx, key, val)
		}(key, values[idx])
	}
	wg.Wait()
	close(err_chan)
	for err := range err_chan {
		if err != nil {
			return err
		}
	}
	return nil
}

// Tomislav-RetCtx: PutRaw stores value byte for byte (memcached set, no JSON encoding), as upstream DeathStarBench's
// memcached_set stores the JSON text it was given. Not part of backend.Cache: workflows reach it through a type assertion.
func (m *Memcached) PutRaw(ctx context.Context, key string, value []byte) error {
	return m.Client.Set(&memcache.Item{Key: key, Value: value})
}

// Tomislav-RetCtx: GetRaw returns the stored bytes as they are (memcached get); found is false on a miss.
func (m *Memcached) GetRaw(ctx context.Context, key string) ([]byte, bool, error) {
	it, err := m.Client.Get(key)
	if err == memcache.ErrCacheMiss {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return it.Value, true, nil
}

// Tomislav-RetCtx: MgetRaw is one memcached multi-get; values[i] holds the bytes stored under keys[i], nil for a miss.
func (m *Memcached) MgetRaw(ctx context.Context, keys []string) ([][]byte, error) {
	items, err := m.Client.GetMulti(keys)
	if err != nil {
		return nil, err
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		if it, ok := items[key]; ok {
			values[i] = it.Value
		}
	}
	return values, nil
}
