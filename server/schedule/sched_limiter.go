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

package schedule

import (
	"container/list"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	log "github.com/sirupsen/logrus"
)

var historyKeepTime = 5 * time.Minute

// HeartbeatStreams is an interface of async region heartbeat.
type HeartbeatStreams interface {
	SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse)
}

// SchedLimiter is used to limit the speed of scheduling.
type SchedLimiter struct {
	sync.RWMutex
	cluster   Cluster
	operators map[uint64]*Operator
	Limiter   *Limiter
	hbStreams HeartbeatStreams
	histories *list.List
}

// NewSchedLimiter creates a SchedLimiter.
func NewSchedLimiter(cluster Cluster, hbStreams HeartbeatStreams) *SchedLimiter {
	return &SchedLimiter{
		cluster:   cluster,
		operators: make(map[uint64]*Operator),
		Limiter:   NewLimiter(),
		hbStreams: hbStreams,
		histories: list.New(),
	}
}

// Dispatch is used to dispatch the operator of a region.
func (s *SchedLimiter) Dispatch(region *core.RegionInfo) {
	// Check existed operator.
	if op := s.GetOperator(region.GetID()); op != nil {
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			s.SendScheduleCommand(region, step)
			return
		}
		if op.IsFinish() {
			log.Infof("[region %v] operator finish: %s", region.GetID(), op)
			operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
			operatorDuration.WithLabelValues(op.Desc()).Observe(op.ElapsedTime().Seconds())
			s.pushHistory(op)
			s.RemoveOperator(op)
		} else if timeout {
			log.Infof("[region %v] operator timeout: %s", region.GetID(), op)
			s.RemoveOperator(op)
		}
	}
}

// AddOperator adds operators to the running operators.
func (s *SchedLimiter) AddOperator(ops ...*Operator) bool {
	s.Lock()
	defer s.Unlock()

	for _, op := range ops {
		if !s.checkAddOperator(op) {
			operatorCounter.WithLabelValues(op.Desc(), "canceled").Inc()
			return false
		}
	}
	for _, op := range ops {
		s.addOperatorLocked(op)
	}

	return true
}

func (s *SchedLimiter) checkAddOperator(op *Operator) bool {
	region := s.cluster.GetRegion(op.RegionID())
	if region == nil {
		log.Debugf("[region %v] region not found, cancel add operator", op.RegionID())
		return false
	}
	if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() || region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
		log.Debugf("[region %v] region epoch not match, %v vs %v, cancel add operator", op.RegionID(), region.GetRegionEpoch(), op.RegionEpoch())
		return false
	}
	if old := s.operators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
		log.Debugf("[region %v] already have operator %s, cancel add operator", op.RegionID(), old)
		return false
	}
	return true
}

func isHigherPriorityOperator(new, old *Operator) bool {
	return new.GetPriorityLevel() < old.GetPriorityLevel()
}

func (s *SchedLimiter) addOperatorLocked(op *Operator) bool {
	regionID := op.RegionID()

	log.Infof("[region %v] add operator: %s", regionID, op)

	// If there is an old operator, replace it. The priority should be checked
	// already.
	if old, ok := s.operators[regionID]; ok {
		log.Infof("[region %v] replace old operator: %s", regionID, old)
		operatorCounter.WithLabelValues(old.Desc(), "replaced").Inc()
		s.removeOperatorLocked(old)
	}

	s.operators[regionID] = op
	s.Limiter.UpdateCounts(s.operators)

	if region := s.cluster.GetRegion(op.RegionID()); region != nil {
		if step := op.Check(region); step != nil {
			s.SendScheduleCommand(region, step)
		}
	}

	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	return true
}

// RemoveOperator removes a operator from the running operators.
func (s *SchedLimiter) RemoveOperator(op *Operator) {
	s.Lock()
	defer s.Unlock()
	s.removeOperatorLocked(op)
}

func (s *SchedLimiter) removeOperatorLocked(op *Operator) {
	regionID := op.RegionID()
	delete(s.operators, regionID)
	s.Limiter.UpdateCounts(s.operators)
	operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
}

// GetOperator gets a operator from the given region.
func (s *SchedLimiter) GetOperator(regionID uint64) *Operator {
	s.RLock()
	defer s.RUnlock()
	return s.operators[regionID]
}

// GetOperators gets operators from the running operators.
func (s *SchedLimiter) GetOperators() []*Operator {
	s.RLock()
	defer s.RUnlock()

	operators := make([]*Operator, 0, len(s.operators))
	for _, op := range s.operators {
		operators = append(operators, op)
	}

	return operators
}

// SendScheduleCommand sends a command to the region.
func (s *SchedLimiter) SendScheduleCommand(region *core.RegionInfo, step OperatorStep) {
	log.Infof("[region %v] send schedule command: %s", region.GetID(), step)
	switch st := step.(type) {
	case TransferLeader:
		cmd := &pdpb.RegionHeartbeatResponse{
			TransferLeader: &pdpb.TransferLeader{
				Peer: region.GetStorePeer(st.ToStore),
			},
		}
		s.hbStreams.SendMsg(region, cmd)
	case AddPeer:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      st.PeerID,
					StoreId: st.ToStore,
				},
			},
		}
		s.hbStreams.SendMsg(region, cmd)
	case AddLearner:
		if region.GetStorePeer(st.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddLearnerNode,
				Peer: &metapb.Peer{
					Id:        st.PeerID,
					StoreId:   st.ToStore,
					IsLearner: true,
				},
			},
		}
		s.hbStreams.SendMsg(region, cmd)
	case PromoteLearner:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				// reuse AddNode type
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      st.PeerID,
					StoreId: st.ToStore,
				},
			},
		}
		s.hbStreams.SendMsg(region, cmd)
	case RemovePeer:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
				Peer:       region.GetStorePeer(st.FromStore),
			},
		}
		s.hbStreams.SendMsg(region, cmd)
	case MergeRegion:
		if st.IsPassive {
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			Merge: &pdpb.Merge{
				Target: st.ToRegion,
			},
		}
		s.hbStreams.SendMsg(region, cmd)
	case SplitRegion:
		cmd := &pdpb.RegionHeartbeatResponse{
			SplitRegion: &pdpb.SplitRegion{
				Policy: st.Policy,
			},
		}
		s.hbStreams.SendMsg(region, cmd)
	default:
		log.Errorf("unknown operatorStep: %v", step)
	}
}

func (s *SchedLimiter) pushHistory(op *Operator) {
	s.Lock()
	defer s.Unlock()
	for _, h := range op.History() {
		s.histories.PushFront(h)
	}
}

// PruneHistory prunes a part of operators' history.
func (s *SchedLimiter) PruneHistory() {
	s.Lock()
	defer s.Unlock()
	p := s.histories.Back()
	for p != nil && time.Since(p.Value.(OperatorHistory).FinishTime) > historyKeepTime {
		prev := p.Prev()
		s.histories.Remove(p)
		p = prev
	}
}

// GetHistory gets operators' history.
func (s *SchedLimiter) GetHistory(start time.Time) []OperatorHistory {
	s.RLock()
	defer s.RUnlock()
	histories := make([]OperatorHistory, 0, s.histories.Len())
	for p := s.histories.Front(); p != nil; p = p.Next() {
		history := p.Value.(OperatorHistory)
		if history.FinishTime.Before(start) {
			break
		}
		histories = append(histories, history)
	}
	return histories
}

// Limiter is a counter that limits the number of operators.
type Limiter struct {
	sync.RWMutex
	counts map[OperatorKind]uint64
}

// NewLimiter creates a schedule limiter.
func NewLimiter() *Limiter {
	return &Limiter{
		counts: make(map[OperatorKind]uint64),
	}
}

// UpdateCounts updates resource counts using current pending operators.
func (l *Limiter) UpdateCounts(operators map[uint64]*Operator) {
	l.Lock()
	defer l.Unlock()
	for k := range l.counts {
		delete(l.counts, k)
	}
	for _, op := range operators {
		l.counts[op.Kind()]++
	}
}

// OperatorCount gets the count of operators filtered by mask.
func (l *Limiter) OperatorCount(mask OperatorKind) uint64 {
	l.RLock()
	defer l.RUnlock()
	var total uint64
	for k, count := range l.counts {
		if k&mask != 0 {
			total += count
		}
	}
	return total
}
