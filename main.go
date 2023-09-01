package main

import (
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

func main() {
	channels := 5
	dataLen := 3
	sleepTime := 500 * time.Millisecond
	sources := make([]<-chan int, 0, channels)

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
}
