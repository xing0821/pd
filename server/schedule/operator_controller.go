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
	log "github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
)

const (
	historyKeepTime      = 5 * time.Minute
	maxScheduleCost      = 200
	storeMaxScheduleCost = 10
)

// HeartbeatStreams is an interface of async region heartbeat.
type HeartbeatStreams interface {
	SendMsg(region *core.RegionInfo, msg *pdpb.RegionHeartbeatResponse)
}

// OperatorController is used to limit the speed of scheduling.
type OperatorController struct {
	sync.RWMutex
	cluster          Cluster
	runningOperators map[uint64]*Operator
	waitingOperators map[uint64]*Operator
	hbStreams        HeartbeatStreams
	histories        *list.List
	counts           map[OperatorKind]uint64
	opInflight       map[string]uint64
	storesCost       map[uint64]uint64
}

// NewOperatorController creates a OperatorController.
func NewOperatorController(cluster Cluster, hbStreams HeartbeatStreams) *OperatorController {
	return &OperatorController{
		cluster:          cluster,
		runningOperators: make(map[uint64]*Operator),
		waitingOperators: make(map[uint64]*Operator),
		hbStreams:        hbStreams,
		histories:        list.New(),
		counts:           make(map[OperatorKind]uint64),
		opInflight:       make(map[string]uint64),
		storesCost:       make(map[uint64]uint64),
	}
}

// Dispatch is used to dispatch the operator of a region.
func (oc *OperatorController) Dispatch(region *core.RegionInfo) {
	// Check existed operator.
	if op := oc.GetRunningOperator(region.GetID()); op != nil {
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			oc.SendScheduleCommand(region, step)
			return
		}
		if op.IsFinish() {
			log.Info("operator finish", zap.Uint64("region-id", region.GetID()), zap.Reflect("operator", op))
			operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
			operatorDuration.WithLabelValues(op.Desc()).Observe(op.ElapsedTime().Seconds())
			oc.pushHistory(op)
			oc.RemoveRunningOperator(op)
		} else if timeout {
			log.Info("operator timeout", zap.Uint64("region-id", region.GetID()), zap.Reflect("operator", op))
			oc.RemoveRunningOperator(op)
		}
	} else if op := oc.GetWaitingOperator(region.GetID()); op != nil {
		timeout := op.IsTimeout()
		if timeout {
			log.Info("operator timeout", zap.Uint64("region-id", region.GetID()), zap.Reflect("operator", op))
			oc.RemoveWaitingOperator(op)
		} else {
			oc.Lock()
			oc.promoteOperator(region, op)
			oc.Unlock()
		}
	}
}

// AddOperator adds operators to the waiting operators.
func (oc *OperatorController) AddOperator(ops ...*Operator) bool {
	oc.Lock()
	defer oc.Unlock()

	for _, op := range ops {
		// If the region can be found and the region epoch is match, there are three cases will return true here:
		// 1. The operator is in running operators, but the priority is higher
		// 2. The operator is in waiting operators, but the priority is higher
		// 3. The operator is not in either the running operators or the waiting operators
		if !oc.checkAddOperator(op) {
			operatorCounter.WithLabelValues(op.Desc(), "canceled").Inc()
			return false
		}
	}
	for _, op := range ops {
		oc.addOperatorLocked(op)
	}

	return true
}

// checkAddOperator checks if the operator can be added.
// There are several situations that cannot be added:
// - There is no such region in the cluster
// - The epoch of the operator and the epoch of the corresponding region are no longer consistent.
// - The region already has a higher priority or same priority operator.
func (oc *OperatorController) checkAddOperator(op *Operator) bool {
	region := oc.cluster.GetRegion(op.RegionID())
	if region == nil {
		log.Debug("region not found, cancel add operator", zap.Uint64("region-id", op.RegionID()))
		return false
	}
	if region.GetRegionEpoch().GetVersion() != op.RegionEpoch().GetVersion() || region.GetRegionEpoch().GetConfVer() != op.RegionEpoch().GetConfVer() {
		log.Debug("region epoch not match, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", region.GetRegionEpoch()), zap.Reflect("new", op.RegionEpoch()))
		return false
	}
	if old := oc.runningOperators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
		log.Debug("already have operator, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", old))
		return false
	}
	if old := oc.waitingOperators[op.RegionID()]; old != nil && !isHigherPriorityOperator(op, old) {
		log.Debug("already have operator, cancel add operator", zap.Uint64("region-id", op.RegionID()), zap.Reflect("old", old))
		return false
	}
	return true
}

func isHigherPriorityOperator(new, old *Operator) bool {
	return new.GetPriorityLevel() < old.GetPriorityLevel()
}

func (oc *OperatorController) addOperatorLocked(op *Operator) {
	regionID := op.RegionID()
	region := oc.cluster.GetRegion(regionID)

	log.Info("add operator", zap.Uint64("region-id", regionID), zap.Reflect("operator", op))
	// If there is an old operator in running operators, replace it.
	// The priority should be checked already.
	if old, ok := oc.runningOperators[regionID]; ok && oc.allowSchedule(old) {
		oc.replaceOperator(oc.runningOperators, op, old)
		if region != nil {
			if step := op.Check(region); step != nil {
				oc.SendScheduleCommand(region, step)
			}
		}
	} else if old, ok := oc.waitingOperators[regionID]; ok {
		// If there is an old operator in waiting operators, replace it.
		// The priority should be checked already.
		oc.replaceOperator(oc.waitingOperators, op, old)
		oc.promoteOperator(region, op)
	} else {
		oc.createOperator(oc.waitingOperators, op)
		oc.promoteOperator(region, op)
	}
}

func (oc *OperatorController) promoteOperator(region *core.RegionInfo, op *Operator) {
	if oc.allowPromote(op) {
		if step := op.Check(region); step != nil {
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			oc.SendScheduleCommand(region, step)
		}
	}
}

func (oc *OperatorController) createOperator(operators map[uint64]*Operator, op *Operator) {
	regionID := op.RegionID()
	operators[regionID] = op
	oc.opInflight[op.Desc()]++
	oc.updateCounts(operators)
	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
}

func (oc *OperatorController) replaceOperator(operators map[uint64]*Operator, op *Operator, old *Operator) {
	regionID := op.RegionID()
	log.Info("replace old operator", zap.Uint64("region-id", regionID), zap.Reflect("operator", old))
	operatorCounter.WithLabelValues(old.Desc(), "replaced").Inc()
	oc.removeOperatorLocked(operators, old)
	operators[regionID] = op
	oc.updateCounts(operators)
}

func (oc *OperatorController) removeOperatorLocked(operators map[uint64]*Operator, op *Operator) {
	regionID := op.RegionID()
	opInfluence := NewOpInfluence([]*Operator{op}, oc.cluster)
	for storeID := range opInfluence.storesInfluence {
		oc.storesCost[storeID] -= opInfluence.GetStoreInfluence(storeID).StepCost
	}
	if _, ok := operators[regionID]; ok {
		delete(operators, regionID)
		oc.opInflight[op.Desc()]--
		oc.updateCounts(operators)
		operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
	}
}

// RemoveRunningOperator removes an operator from running operators.
func (oc *OperatorController) RemoveRunningOperator(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.removeOperatorLocked(oc.runningOperators, op)
}

// RemoveWaitingOperator removes an operator from waiting operators.
func (oc *OperatorController) RemoveWaitingOperator(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.removeOperatorLocked(oc.waitingOperators, op)
}

// GetRunningOperator gets an operator from the given region in running operators.
func (oc *OperatorController) GetRunningOperator(regionID uint64) *Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.runningOperators[regionID]
}

// GetWaitingOperator gets an operator from the given region in waiting operators.
func (oc *OperatorController) GetWaitingOperator(regionID uint64) *Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.waitingOperators[regionID]
}

// GetRunningOperators gets operators from the running operators.
func (oc *OperatorController) GetRunningOperators() []*Operator {
	oc.RLock()
	defer oc.RUnlock()
	return oc.getRunningOperatorsLocked()
}

func (oc *OperatorController) getRunningOperatorsLocked() []*Operator {
	operators := make([]*Operator, 0, len(oc.runningOperators))
	for _, op := range oc.runningOperators {
		operators = append(operators, op)
	}
	return operators
}

// GetWaitingOperators gets operators from the waiting operators.
func (oc *OperatorController) GetWaitingOperators() []*Operator {
	oc.RLock()
	defer oc.RUnlock()

	operators := make([]*Operator, 0, len(oc.waitingOperators))
	for _, op := range oc.waitingOperators {
		operators = append(operators, op)
	}

	return operators
}

func (oc *OperatorController) allowPromote(op *Operator) bool {
	if op.Desc() == "merge-region" {
		for _, step := range op.steps {
			if st, ok := step.(MergeRegion); ok {
				var id uint64
				if st.IsPassive {
					id = st.FromRegion.GetId()
				} else {
					id = st.ToRegion.GetId()
				}
				if op1, ok := oc.waitingOperators[id]; ok &&
					op1.Desc() == "merge-region" && oc.allowSchedule(op, op1) {
					oc.promote(op, op1)
					return true
				}
				return false
			}
		}
	} else if oc.allowSchedule(op) {
		oc.promote(op)
		return true
	}
	return false
}

func (oc *OperatorController) promote(ops ...*Operator) {
	for _, op := range ops {
		regionID := op.RegionID()
		oc.opInflight[op.Desc()]++
		oc.runningOperators[regionID] = op
		oc.removeOperatorLocked(oc.waitingOperators, op)
		operatorCounter.WithLabelValues(op.Desc(), "promote").Inc()
	}
	opInfluence := NewOpInfluence(ops, oc.cluster)
	for storeID := range opInfluence.storesInfluence {
		oc.storesCost[storeID] += opInfluence.GetStoreInfluence(storeID).StepCost
	}
	oc.updateCounts(oc.runningOperators)
}

func (oc *OperatorController) allowSchedule(ops ...*Operator) bool {
	if oc.getScheduleCost() > maxScheduleCost {
		return false
	}
	if oc.exceedStoreCost(ops...) {
		return false
	}
	return true
}

// SendScheduleCommand sends a command to the region.
func (oc *OperatorController) SendScheduleCommand(region *core.RegionInfo, step OperatorStep) {
	log.Info("send schedule command", zap.Uint64("region-id", region.GetID()), zap.Reflect("step", step))
	switch st := step.(type) {
	case TransferLeader:
		cmd := &pdpb.RegionHeartbeatResponse{
			TransferLeader: &pdpb.TransferLeader{
				Peer: region.GetStorePeer(st.ToStore),
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
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
		oc.hbStreams.SendMsg(region, cmd)
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
		oc.hbStreams.SendMsg(region, cmd)
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
		oc.hbStreams.SendMsg(region, cmd)
	case RemovePeer:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
				Peer:       region.GetStorePeer(st.FromStore),
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case MergeRegion:
		if st.IsPassive {
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			Merge: &pdpb.Merge{
				Target: st.ToRegion,
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	case SplitRegion:
		cmd := &pdpb.RegionHeartbeatResponse{
			SplitRegion: &pdpb.SplitRegion{
				Policy: st.Policy,
			},
		}
		oc.hbStreams.SendMsg(region, cmd)
	default:
		log.Error("unknown operator step", zap.Reflect("step", step))
	}
}

func (oc *OperatorController) pushHistory(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	for _, h := range op.History() {
		oc.histories.PushFront(h)
	}
}

// PruneHistory prunes a part of operators' history.
func (oc *OperatorController) PruneHistory() {
	oc.Lock()
	defer oc.Unlock()
	p := oc.histories.Back()
	for p != nil && time.Since(p.Value.(OperatorHistory).FinishTime) > historyKeepTime {
		prev := p.Prev()
		oc.histories.Remove(p)
		p = prev
	}
}

// GetHistory gets operators' history.
func (oc *OperatorController) GetHistory(start time.Time) []OperatorHistory {
	oc.RLock()
	defer oc.RUnlock()
	histories := make([]OperatorHistory, 0, oc.histories.Len())
	for p := oc.histories.Front(); p != nil; p = p.Next() {
		history := p.Value.(OperatorHistory)
		if history.FinishTime.Before(start) {
			break
		}
		histories = append(histories, history)
	}
	return histories
}

// updateCounts updates resource counts using current pending operators.
func (oc *OperatorController) updateCounts(operators map[uint64]*Operator) {
	for k := range oc.counts {
		delete(oc.counts, k)
	}
	for _, op := range operators {
		oc.counts[op.Kind()]++
	}
}

// OperatorCount gets the count of operators filtered by mask.
func (oc *OperatorController) OperatorCount(mask OperatorKind) uint64 {
	oc.RLock()
	defer oc.RUnlock()
	var total uint64
	for k, count := range oc.counts {
		if k&mask != 0 {
			total += count
		}
	}
	return total
}

// GetOpInfluence gets OpInfluence.
func (oc *OperatorController) GetOpInfluence(cluster Cluster) OpInfluence {
	oc.RLock()
	defer oc.RUnlock()

	var res []*Operator
	operators := oc.getRunningOperatorsLocked()
	for _, op := range operators {
		if !op.IsTimeout() && !op.IsFinish() {
			region := cluster.GetRegion(op.RegionID())
			if region != nil {
				res = append(res, op)
			}
		}
	}
	return NewOpInfluence(res, cluster)
}

// NewOpInfluence creates a OpInfluence.
func NewOpInfluence(operators []*Operator, cluster Cluster) OpInfluence {
	influence := OpInfluence{
		storesInfluence:  make(map[uint64]*StoreInfluence),
		regionsInfluence: make(map[uint64]*Operator),
	}

	for _, op := range operators {
		region := cluster.GetRegion(op.RegionID())
		if region != nil {
			op.Influence(influence, region)
		}
		influence.regionsInfluence[op.RegionID()] = op
	}

	return influence
}

// OpInfluence records the influence of the cluster.
type OpInfluence struct {
	storesInfluence  map[uint64]*StoreInfluence
	regionsInfluence map[uint64]*Operator
}

// GetStoreInfluence get storeInfluence of specific store.
func (m OpInfluence) GetStoreInfluence(id uint64) *StoreInfluence {
	storeInfluence, ok := m.storesInfluence[id]
	if !ok {
		storeInfluence = &StoreInfluence{}
		m.storesInfluence[id] = storeInfluence
	}
	return storeInfluence
}

// GetRegionsInfluence gets regionInfluence of specific region.
func (m OpInfluence) GetRegionsInfluence() map[uint64]*Operator {
	return m.regionsInfluence
}

// StoreInfluence records influences that pending operators will make.
type StoreInfluence struct {
	RegionSize  int64
	RegionCount int64
	LeaderSize  int64
	LeaderCount int64
	StepCost    uint64
}

// ResourceSize returns delta size of leader/region by influence.
func (s StoreInfluence) ResourceSize(kind core.ResourceKind) int64 {
	switch kind {
	case core.LeaderKind:
		return s.LeaderSize
	case core.RegionKind:
		return s.RegionSize
	default:
		return 0
	}
}

// SetOperator is only used for test
func (oc *OperatorController) SetOperator(op *Operator) {
	oc.Lock()
	defer oc.Unlock()
	oc.runningOperators[op.RegionID()] = op
}

// OperatorInflight gets the count of operators for a given scheduler or checker.
func (oc *OperatorController) OperatorInflight(name string) uint64 {
	oc.RLock()
	defer oc.RUnlock()
	return oc.opInflight[name]
}

// exceedStoreCost return true if store exceeds the cost after adding the operator. Otherwise, return false.
func (oc *OperatorController) exceedStoreCost(ops ...*Operator) bool {
	opInfluence := NewOpInfluence(ops, oc.cluster)
	for storeID := range opInfluence.storesInfluence {
		if oc.storesCost[storeID]+opInfluence.GetStoreInfluence(storeID).StepCost > storeMaxScheduleCost {
			return true
		}
	}
	return false
}

func (oc *OperatorController) getScheduleCost() uint64 {
	var scheduleCost uint64
	for _, cost := range oc.storesCost {
		scheduleCost += cost
	}
	return scheduleCost
}
