package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
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
	MultipleCandidates
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
	id     int
	Input  chan Message
	Output chan Message
}

var messageCounter int32 = 0
var verbose = false

func receive(v *Node) bool {
	message := <-v.Input
	return message.Sent
}

func send(v *Node, val bool) {
	if val {
		atomic.AddInt32(&messageCounter, 1)
		if verbose {
			fmt.Println(v.id, " wysłał wiadomość")
		}
	}
	v.Output <- Message{val}
}

// returns true if finished
func process(v *Node) bool {
	state := &v.State
	if state.Phase == Phase1 {
		if state.RoundInPhase == 0 {
			if verbose {
				fmt.Println("Początek fazy 1")
			}
			state.CandidateExists = false
			if rand.Float64() < state.c/float64(state.N) {
				state.Status = Candidate
				if verbose {
					fmt.Println("Wybrano kandydata")
				}
				send(v, true)
			} else {
				state.Status = Nothing
				send(v, false)
			}
		}
		message := receive(v)
		if message {
			state.CandidateExists = true
		}

		state.RoundInPhase++
		if state.RoundInPhase == (2*state.N - 1) {
			if state.CandidateExists {
				state.Phase = Phase2_1
			}
			state.RoundInPhase = 0
		} else {
			if state.Status == Candidate {
				send(v, false)
			} else {
				send(v, message)
			}
		}
	} else if state.Phase == Phase2_1 {
		if state.RoundInPhase == 0 {
			if verbose {
				fmt.Println("Początek fazy 2.1")
			}
			state.MultipleLeaders = false
			if state.Status == Candidate {
				send(v, true)
			} else {
				send(v, false)
			}
		}
		message := receive(v)
		if state.Status == Candidate {
			if message {
				state.MultipleLeaders = true
				state.Status = MultipleCandidates
				if verbose {
					fmt.Println("Wykryto wielu liderów ")
				}
			}
		}

		state.RoundInPhase++
		if state.RoundInPhase == (state.N - 1) {
			state.RoundInPhase = 0
			state.Phase = Phase2_2
		} else {
			if state.Status == Candidate || state.Status == MultipleCandidates {
				send(v, false)
			} else {
				send(v, message)
			}
		}
	} else if state.Phase == Phase2_2 {
		if state.RoundInPhase == 0 {
			if verbose {
				fmt.Println("Początek fazy 2.2")
			}
			if state.MultipleLeaders {
				send(v, true)
			} else {
				send(v, false)
			}
		}
		message := receive(v)
		if message {
			if !state.MultipleLeaders && verbose {
				fmt.Println("Otrzymano informację o wielu kandydatach")
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
					if verbose {
						fmt.Println("Lider wyznaczony")
					}

				}
				return true
			}
		} else {
			if state.Status == MultipleCandidates {
				send(v, false)
			} else {
				send(v, message)
			}
		}
	}
	return false
}

type Barrier struct {
	mu      sync.Mutex
	n       int
	arrived int
	ch      chan struct{}
}

func NewBarrier(n int) Barrier {
	return Barrier{
		n:  n,
		ch: make(chan struct{}),
	}
}

func (b *Barrier) Wait() {
	b.mu.Lock()

	ch := b.ch
	b.arrived++

	if b.arrived == b.n {
		b.arrived = 0
		b.ch = make(chan struct{})
		close(ch)
	}

	b.mu.Unlock()
	<-ch
}

func run(v *Node, barrier1 *Barrier, barrier2 *Barrier) {
	finished := false
	for round := 0; !finished; round++ {
		finished = process(v)
		barrier1.Wait()
		barrier2.Wait()
	}
}

// Returns (number of turns, number of messages)
func electLeader(N int, n int, c float64) (int, int) {
	messageCounter = 0
	nodes := make([]Node, n)
	channels := make([]chan Message, n)
	for i := 0; i < n; i++ {
		channels[i] = make(chan Message, 1)
		// channels[i] <- Message{false}
	}
	for i := 0; i < n; i++ {
		state := State{Phase1, 0, Nothing, N, c, false, false}
		nodes[i] = Node{state, i, channels[i], channels[(i+1)%n]}
	}
	barrier1 := NewBarrier(n + 1)
	barrier2 := NewBarrier(n + 1)
	//start all the processes
	for i := 0; i < n; i++ {
		go run(&nodes[i], &barrier1, &barrier2)
	}
	//process synchronization
	finished := false
	round := 1
	for ; !finished; round++ {
		if verbose {
			fmt.Println("Runda ", round)
		}
		barrier1.Wait()
		for i := 0; i < n; i++ {
			if nodes[i].State.Phase == Finished {
				finished = true
			}
		}
		barrier2.Wait()
	}
	cnt := 0
	for i := 0; i < n; i++ {
		if nodes[i].State.Status == Leader {
			if verbose {
				fmt.Println("Wybrany lider ", i)
			}
			cnt++
		}
	}
	if cnt != 1 {
		panic("Wybór lidera nieudany! ")
	}
	return round, int(messageCounter)
}

func main() {
	flag.BoolVar(&verbose, "verbose", false, "wypisuj szczegółowy przebieg algorytmu")
	flag.Parse()
	var c float64
	var n, N, t int
	fmt.Println("Podaj (N, n, c, t - liczba eksperymentów)")
	fmt.Scan(&N, &n, &c, &t)
	if !(N <= n && n <= 2*N-1) {
		panic("Aproximation is wrong")
	}
	totalRounds := 0
	totalMessages := 0
	for i := 0; i < t; i++ {
		rounds, messages := electLeader(N, n, c)
		totalRounds += rounds
		totalMessages += messages
		fmt.Println("Ekpseryment ", i)
	}
	avgRounds := float64(totalRounds) / float64(t)
	avgMessages := float64(totalMessages) / float64(t)
	fN, fn := float64(N), float64(n)
	// Ograniczenia (po uproszczeniach)
	kSel := 1.0 / (1.0 - math.Exp(-c))
	kAlg := (math.Exp(2*c+(c*c)/(fN-c)) - 1) / c
	theoryRounds := kAlg * (3.0*fN - 2.0 + kSel*(2.0*fN-1))
	theoryMessages := (3.0*kAlg-2)*fn + fN - 1

	// Dokładne wartości przed uproszczeniami (q = 1 - c/N):
	//   Pr_SEL = 1 - q^n
	//   Pr_ALG = n(c/N)q^(n-1) / (1 - q^n)
	q := 1.0 - c/fN
	exactKSel := 1.0 / (1.0 - math.Pow(q, fn))
	exactKAlg := (1.0 - math.Pow(q, fn)) / (fn * (c / fN) * math.Pow(q, fn-1))
	exactRounds := exactKAlg * (3.0*fN - 2.0 + exactKSel*(2.0*fN-1))
	exactMessages := (3.0*exactKAlg-2)*fn + fN - 1

	fmt.Println("Średnia zmierzona liczba rund = ", avgRounds, "(wartość teoretyczna ", exactRounds, ", ograniczenie ", theoryRounds, ")")
	fmt.Println("Średnia zmierzona liczba wiadomości = ", avgMessages, "(wartość teoretyczna ", exactMessages, ", ograniczenie ", theoryMessages, ")")
}
