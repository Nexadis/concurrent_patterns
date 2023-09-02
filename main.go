package main

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sync"
	"time"
)

func FanIn(sources ...<-chan int) <-chan int {
	dest := make(chan int)

	var wg sync.WaitGroup

	wg.Add(len(sources))

	for i, s := range sources {
		i := i
		fmt.Printf("Job %d is run\n", i)
		go func(s <-chan int) {
			defer func() {
				fmt.Printf("Job %d is done\n", i)
				wg.Done()
			}()
			for v := range s {
				dest <- v
			}
		}(s)
	}

	go func() {
		fmt.Println("Begin wait")
		wg.Wait()
		fmt.Println("End wait")

		close(dest)
	}()

	return dest
}

func FanOut(source <-chan int, n int) []<-chan int {
	destinations := make([]<-chan int, 0, n)
	for i := 0; i < n; i++ {
		i := i
		fmt.Printf("Job %d is run\n", i)
		dest := make(chan int)
		destinations = append(destinations, dest)
		go func() {
			defer close(dest)
			for val := range source {
				dest <- val
			}
			fmt.Printf("Job %d is done\n", i)
		}()
	}
	return destinations
}

type Future interface {
	Result() (int, error)
}

type InnerFuture struct {
	once sync.Once
	wg   sync.WaitGroup

	res   int
	err   error
	resCh <-chan int
	errCh <-chan error
}

func (f *InnerFuture) Result() (int, error) {
	f.once.Do(func() {
		f.wg.Add(1)
		defer f.wg.Done()
		f.res = <-f.resCh
		f.err = <-f.errCh
	})
	f.wg.Wait()

	return f.res, f.err
}

func SlowFunction(ctx context.Context) Future {
	resCh := make(chan int)
	errCh := make(chan error)

	go func() {
		fmt.Println("Wait for future")
		select {
		case <-time.After(time.Second * 1):
			resCh <- 1337
			errCh <- nil
		case <-ctx.Done():
			resCh <- 0
			errCh <- ctx.Err()
		}
	}()
	return &InnerFuture{resCh: resCh, errCh: errCh}
}

type Shard struct {
	sync.RWMutex
	m map[string]interface{}
}

type ShardedMap []*Shard

func NewShardedMap(nshards int) ShardedMap {
	shards := make([]*Shard, nshards)
	for i := 0; i < nshards; i++ {
		shard := make(map[string]interface{})
		shards[i] = &Shard{m: shard}
	}
	return shards
}

func (m ShardedMap) getShardIndex(key string) int {
	checksum := sha1.Sum([]byte(key)) // медленный алгоритм для хеширования
	hash := int(checksum[0])          // берём произвольный байт для хеша
	return hash % len(m)
}

func (m ShardedMap) getShard(key string) *Shard {
	index := m.getShardIndex(key)
	return m[index]
}

func (m ShardedMap) Get(key string) (interface{}, bool) {
	shard := m.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	val, ok := shard.m[key]

	return val, ok
}

func (m ShardedMap) Set(key string, value interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()

	shard.m[key] = value
}

func (m ShardedMap) Keys() []string {
	keys := make([]string, 0, len(m)) // предвыделяем память, точное количество ключей неизвестно, но попробуем минимизировать начальные расходы

	mutex := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(m))
	for _, shard := range m {
		go func(s *Shard) {
			s.RLock()
			for key := range s.m {
				mutex.Lock()
				keys = append(keys, key)
				mutex.Unlock()
			}

			s.RUnlock()
			wg.Done()
		}(shard)
	}
	wg.Wait()

	return keys
}

func main() {
	channels := 5
	dataLen := 3
	sleepTime := 500 * time.Millisecond
	sources := make([]<-chan int, 0, channels)
	border("[ Fan in ]")

	for i := 0; i < channels; i++ {
		ch := make(chan int)
		sources = append(sources, ch)
		i := i
		go func() {
			defer func() {
				fmt.Printf("Channel %d is closed\n", i)
				close(ch)
			}()
			for i := 1; i <= dataLen; i++ {
				ch <- i
				time.Sleep(sleepTime)
			}
		}()
	}
	dest := FanIn(sources...)
	for d := range dest {
		fmt.Println(d)
	}
	border("[ Fan out ]")

	source := make(chan int)
	dests := FanOut(source, channels)
	go func() {
		for i := 1; i <= dataLen*channels; i++ {
			source <- i
		}
		close(source)
	}()

	var wg sync.WaitGroup
	wg.Add(len(dests))

	for i, ch := range dests {
		go func(i int, d <-chan int) {
			defer wg.Done()

			for val := range d {
				fmt.Printf("[%d] got %d\n", i, val)
			}
		}(i, ch)
	}
	wg.Wait()

	border("[ Future ]")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1500)
	defer cancel()

	future := SlowFunction(ctx)
	res, err := future.Result()
	if err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Println(res)
	}
	res, err = future.Result()
	if err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Println(res)
	}
	border("[ Sharding ]")
	shardedMap := NewShardedMap(3)
	for i := 0; i < 9; i++ {
		word := fmt.Sprintf("%c", 'a'+i)
		shardedMap.Set(word, i)
	}
	keys := shardedMap.Keys()

	for _, k := range keys {
		fmt.Println(k)
	}
}

func border(msg string) {
	for i := 0; i < 80; i++ {
		fmt.Print("-")
	}
	fmt.Println()

	fmt.Println(msg)

	for i := 0; i < 80; i++ {
		fmt.Print("-")
	}
	fmt.Println()
}
