package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
)

type Message struct {
	Sent bool
}

type Phase int

const (
	Phase1 Phase = iota
	Phase2_1
	Phase2_2
	Finished
)

type Status int

const (
	Nothing Status = iota
	Candidate
	Leader
)

type State struct {
	Phase           Phase
	RoundInPhase    int
	Status          Status
	N               int
	c               float64
	CandidateExists bool
	MultipleLeaders bool
}

type Node struct {
	State  State
	Input  chan Message
	Output chan Message
}

// returns true if finished
func process(v *Node, wg *sync.WaitGroup) {
	state := &v.State
	if state.Phase == Phase1 {
		if state.RoundInPhase == 0 {
			fmt.Println("Start Phase1")
			state.CandidateExists = false
			if rand.Float64() < state.c/float64(state.N) {
				state.Status = Candidate
				fmt.Println("Candidate selected")
				v.Output <- Message{true}
			} else {
				state.Status = Nothing
				v.Output <- Message{false}
			}
		}
		message := <-v.Input
		if message.Sent {
			state.CandidateExists = true
		}

		state.RoundInPhase++
		if state.RoundInPhase == (2*state.N - 1) {
			if state.CandidateExists {
				state.Phase = Phase2_1
			}
			state.RoundInPhase = 0
		} else {
			v.Output <- message
		}
	} else if state.Phase == Phase2_1 {
		if state.RoundInPhase == 0 {
			fmt.Println("Start Phase 2.1")
			state.MultipleLeaders = false
			if state.Status == Candidate {
				v.Output <- Message{true}
			} else {
				v.Output <- Message{false}
			}
		}
		message := <-v.Input
		if state.Status == Candidate {
			if message.Sent {
				state.MultipleLeaders = true
				fmt.Println("Multiple leaders detected")
			}
			message = Message{false}
		}

		state.RoundInPhase++
		if state.RoundInPhase == (state.N - 1) {
			state.RoundInPhase = 0
			state.Phase = Phase2_2
		} else {
			v.Output <- message
		}
	} else if state.Phase == Phase2_2 {
		if state.RoundInPhase == 0 {
			fmt.Println("Start Phase 2.2")
			if state.MultipleLeaders {
				v.Output <- Message{true}
			} else {
				v.Output <- Message{false}
			}
		}
		message := <-v.Input
		if message.Sent {
			if !state.MultipleLeaders {
				fmt.Println("Received information about multiple leaders")
			}
			state.MultipleLeaders = true
		}

		state.RoundInPhase++
		if state.RoundInPhase == (2*state.N - 1) {
			state.RoundInPhase = 0
			if state.MultipleLeaders {
				state.Phase = Phase1
			} else {
				state.Phase = Finished
				if state.Status == Candidate {
					state.Status = Leader
					fmt.Println("Leader selected")
				}
			}
		} else {
			v.Output <- message
		}
	}
	wg.Done()
}

func main() {
	var c float64
	var n, N int
	fmt.Scan(&N, &n, &c)
	if !(N <= n && n <= 2*N-1) {
		panic("Aproximation is wrong")
	}
	nodes := make([]Node, n)
	channels := make([]chan Message, n)
	for i := 0; i < n; i++ {
		channels[i] = make(chan Message, 1)
		// channels[i] <- Message{false}
	}

	for i := 0; i < n; i++ {
		state := State{Phase1, 0, Nothing, N, c, false, false}
		nodes[i] = Node{state, channels[i], channels[(i+1)%n]}
	}
	finished := false
	for round := 0; !finished; round++ {
		fmt.Println("Round ", round)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go process(&nodes[i], &wg)
		}
		wg.Wait()
		finished = true
		for i := 0; i < n; i++ {
			if nodes[i].State.Phase != Finished {
				finished = false
			}
		}
	}
}
