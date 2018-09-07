// Copyright 2018 PingCAP, Inc.
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

package faketikv

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/faketikv/cases"
	"github.com/pingcap/pd/pkg/faketikv/simutil"
	"github.com/pingcap/pd/server/core"
)

// Event that affect the status of the cluster
type Event interface {
	Run(er *EventRunner, tickCount int64) bool
}

// EventRunner includes all events
type EventRunner struct {
	events     []Event
	raftEngine *RaftEngine
}

// NewEventRunner creates an event runner
func NewEventRunner(events []cases.EventInner, raftEngine *RaftEngine) *EventRunner {
	er := &EventRunner{events: make([]Event, 0, len(events)), raftEngine: raftEngine}
	for _, e := range events {
		event := parserEvent(e)
		if event != nil {
			er.events = append(er.events, event)
		}
	}
	return er
}

func parserEvent(e cases.EventInner) Event {
	switch v := e.(type) {
	case *cases.WriteFlowOnSpotInner:
		return &WriteFlowOnSpot{in: v}
	case *cases.WriteFlowOnRegionInner:
		return &WriteFlowOnRegion{in: v}
	case *cases.ReadFlowOnRegionInner:
		return &ReadFlowOnRegion{in: v}
	case *cases.AddNodesDynamicInner:
		return &AddNodesDynamic{in: v}
	case *cases.DeleteNodesInner:
		return &DeleteNodes{in: v}
	}
	return nil
}

// Tick ticks the event run
func (er *EventRunner) Tick(tickCount int64) {
	var finishedIndex int
	for i, e := range er.events {
		isFinished := e.Run(er, tickCount)
		if isFinished {
			er.events[i], er.events[finishedIndex] = er.events[finishedIndex], er.events[i]
			finishedIndex++
		}
	}
	er.events = er.events[finishedIndex:]
}

// WriteFlowOnSpot writes bytes in some range
type WriteFlowOnSpot struct {
	in *cases.WriteFlowOnSpotInner
}

// Run implements the event interface
func (w *WriteFlowOnSpot) Run(er *EventRunner, tickCount int64) bool {
	raft := er.raftEngine
	res := w.in.Step(tickCount)
	for key, size := range res {
		region := raft.SearchRegion([]byte(key))
		if region == nil {
			simutil.Logger.Errorf("region not found for key %s", key)
			continue
		}
		raft.updateRegionStore(region, size)
	}
	return false
}

// WriteFlowOnRegion writes bytes in some region
type WriteFlowOnRegion struct {
	in *cases.WriteFlowOnRegionInner
}

// Run implements the event interface
func (w *WriteFlowOnRegion) Run(er *EventRunner, tickCount int64) bool {
	raft := er.raftEngine
	res := w.in.Step(tickCount)
	for id, bytes := range res {
		region := raft.GetRegion(id)
		if region == nil {
			simutil.Logger.Errorf("region %d not found", id)
			continue
		}
		raft.updateRegionStore(region, bytes)
	}
	return false
}

// ReadFlowOnRegion reads bytes in some region
type ReadFlowOnRegion struct {
	in *cases.ReadFlowOnRegionInner
}

// Run implements the event interface
func (w *ReadFlowOnRegion) Run(er *EventRunner, tickCount int64) bool {
	res := w.in.Step(tickCount)
	er.raftEngine.updateRegionReadBytes(res)
	return false
}

// AddNodesDynamic adds nodes dynamically.
type AddNodesDynamic struct {
	in *cases.AddNodesDynamicInner
}

// Run implements the event interface.
func (w *AddNodesDynamic) Run(er *EventRunner, tickCount int64) bool {
	res := w.in.Step(tickCount)
	if res == 0 {
		return false
	}
	er.AddNode(res)
	return false
}

// DeleteNodes deletes nodes randomly
type DeleteNodes struct {
	in *cases.DeleteNodesInner
}

// Run implements the event interface
func (w *DeleteNodes) Run(er *EventRunner, tickCount int64) bool {
	res := w.in.Step(tickCount)
	if res == 0 {
		return false
	}
	er.DeleteNode(res)
	return false
}

// AddNode adds a new node.
func (er *EventRunner) AddNode(id uint64) {
	raft := er.raftEngine
	if _, ok := raft.conn.Nodes[id]; ok {
		simutil.Logger.Infof("Node %d already existed", id)
		return
	}
	s := &cases.Store{
		ID:        id,
		Status:    metapb.StoreState_Up,
		Capacity:  1 * cases.TB,
		Available: 1 * cases.TB,
		Version:   "2.1.0",
	}
	n, err := NewNode(s, raft.conn.pdAddr)
	if err != nil {
		simutil.Logger.Errorf("Add node %d failed: %v", id, err)
		return
	}
	raft.conn.Nodes[id] = n
	n.raftEngine = raft
	err = n.Start()
	if err != nil {
		simutil.Logger.Errorf("Start node %d failed: %v", id, err)
	}
}

// DeleteNode deletes a node.
func (er *EventRunner) DeleteNode(id uint64) {
	raft := er.raftEngine
	node := raft.conn.Nodes[id]
	if node == nil {
		simutil.Logger.Errorf("Node %d not existed", id)
		return
	}
	delete(raft.conn.Nodes, id)
	node.Stop()

	regions := raft.GetRegions()
	for _, region := range regions {
		storeIDs := region.GetStoreIds()
		if _, ok := storeIDs[id]; ok {
			downPeer := &pdpb.PeerStats{
				Peer:        region.GetStorePeer(id),
				DownSeconds: 24 * 60 * 60,
			}
			region = region.Clone(core.WithDownPeers(append(region.GetDownPeers(), downPeer)))
			raft.SetRegion(region)
		}
	}
}
