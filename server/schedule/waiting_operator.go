// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import (
	"container/heap"
	"math"
	"math/rand"
	"time"
)

const maxPriority = 2

// WaitingOperator is an interface of waiting operator.
type WaitingOperator interface {
	PutOperator(op *Operator)
	GetOperator() *Operator
}

// An Item is something we manage in a priority queue.
type Item struct {
	op       *Operator
	priority int
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	items []*Item
}

// NewPriorityQueue create a PriorityQueue.
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{}
}

// PutOperator puts an operator into the priority queue.
func (pq *PriorityQueue) PutOperator(op *Operator) {
	item := &Item{op: op, priority: maxPriority - int(op.GetPriorityLevel())}
	heap.Push(pq, item)
}

// GetOperator gets an operator from the priority queue.
func (pq *PriorityQueue) GetOperator() *Operator {
	if pq.Len() > 0 {
		item := heap.Pop(pq)
		return item.(*Item).op
	}
	return nil
}

func (pq *PriorityQueue) Len() int { return len(pq.items) }

func (pq *PriorityQueue) Less(i, j int) bool {
	if pq.items[i].priority == pq.items[j].priority &&
		pq.items[i].op.createTime.Sub(pq.items[j].op.createTime) < 0 {
		return true
	}
	return pq.items[i].priority > pq.items[j].priority
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

// Push pushes an item into the priority queue.
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)
	pq.items = append(pq.items, item)
}

// Pop pops an item from the priority queue.
func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	pq.items = old[0 : n-1]
	return item
}

// RandQueue is an implementation of waiting operators
type RandQueue struct {
	weight int
	ops    []*Operator
}

// NewRandQueue create a random queue.
func NewRandQueue() *RandQueue {
	return &RandQueue{}
}

// PutOperator puts an operator into the random queue.
func (r *RandQueue) PutOperator(op *Operator) {
	r.ops = append(r.ops, op)
	r.weight += int(math.Pow10((maxPriority - int(op.GetPriorityLevel())) * 2))
}

// GetOperator gets an operator from the random queue.
func (r *RandQueue) GetOperator() *Operator {
	if len(r.ops) == 0 {
		return nil
	}
	rand.Seed(time.Now().UnixNano())
	r1 := rand.Intn(r.weight)
	sum := 0
	for i, op := range r.ops {
		weight := int(math.Pow10((maxPriority - int(op.GetPriorityLevel())) * 2))
		if r1 >= sum && r1 < sum+weight {
			ret := r.ops[i]
			r.ops = append(r.ops[:i], r.ops[i+1:]...)
			r.weight -= weight
			return ret
		}
		sum += weight
	}
	return nil
}
