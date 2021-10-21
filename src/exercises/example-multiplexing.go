package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Message struct {
	str  string
	wait chan bool
}

func boring(msg string) <-chan string {
	c := make(chan string)
	go func() {
		for i := 0; ; i++ {
			c <- fmt.Sprintf("%s %d", msg, i)
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
		}
	}() // Call anonymous function
	return c // Return channel to the caller
}

func fanIn(input1, input2 <-chan string) <-chan string {
	c := make(chan string)
	go func() {
		for {
			c <- <-input1
		}
	}()
	go func() {
		for {
			c <- <-input2
		}
	}()
	return c
}

// This function does the same but only with one go-routine
func fanInWithSelector(input1, input2 <-chan string) <-chan string {
	c := make(chan string)
	go func() {
		for {
			// select will block until one of the case statements can proceed
			// this happens when the channel can give a message. If both channels are ready
			// one of them will execute pseudo randomly
			select {
			case s := <-input1:
				c <- s
			case s := <-input2:
				c <- s
			}
		}
	}()
	return c
}

func multiplexor() {
	c := fanIn(boring("joe"), boring("Ann"))
	for i := 0; i < 10; i++ {
		fmt.Println(<-c)
	}
	fmt.Println("I'm done!")
}

func boringSync(msg string) <-chan Message {
	waitForIt := make(chan bool)
	c := make(chan Message)
	go func() {
		for i := 0; ; i++ {
			c <- Message{fmt.Sprintf("%s %d", msg, i), waitForIt}
			time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond)
			<-waitForIt
		}
	}() // Call anonymous function
	return c // Return channel to the caller
}

func fanInSync(input1, input2 <-chan Message) <-chan Message {
	c := make(chan Message)
	go func() {
		for {
			c <- <-input1
		}
	}() // c <- X, send X to channel c. <-input1, get something from channel
	go func() {
		for {
			c <- <-input2
		}
	}()
	return c
}

func syncMultiplexor() {
	c := fanInSync(boringSync("Joe"), boringSync("Ann"))

	for i := 0; i < 5; i++ {
		msg1 := <-c
		fmt.Println(msg1.str)
		msg2 := <-c
		fmt.Println(msg2.str)
		msg1.wait <- true
		msg2.wait <- true
	}
}

/* source:
https://talks.golang.org/2012/concurrency.slide#30
https://www.youtube.com/watch?v=f6kdp27TYZs */
func main() {
	multiplexor()
	syncMultiplexor()
}
