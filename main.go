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
