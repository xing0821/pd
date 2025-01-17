// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/statistics"
	"go.uber.org/zap"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("hot-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("hot-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {

		return newBalanceHotRegionsScheduler(opController), nil
	})
	// FIXME: remove this two schedule after the balance test move in schedulers package
	schedule.RegisterScheduler("hot-write-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceHotWriteRegionsScheduler(opController), nil
	})
	schedule.RegisterScheduler("hot-read-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceHotReadRegionsScheduler(opController), nil
	})
}

const (
	hotRegionLimitFactor    = 0.75
	storeHotPeersDefaultLen = 100
	hotRegionScheduleFactor = 0.9
	balanceHotRegionName    = "balance-hot-region-scheduler"
)

// BalanceType : the perspective of balance
type BalanceType int

const (
	hotWriteRegionBalance BalanceType = iota
	hotReadRegionBalance
)

type storeStatistics struct {
	readStatAsLeader  statistics.StoreHotPeersStat
	writeStatAsPeer   statistics.StoreHotPeersStat
	writeStatAsLeader statistics.StoreHotPeersStat
}

func newStoreStaticstics() *storeStatistics {
	return &storeStatistics{
		readStatAsLeader:  make(statistics.StoreHotPeersStat),
		writeStatAsLeader: make(statistics.StoreHotPeersStat),
		writeStatAsPeer:   make(statistics.StoreHotPeersStat),
	}
}

type balanceHotRegionsScheduler struct {
	name string
	*baseScheduler
	sync.RWMutex
	leaderLimit uint64
	peerLimit   uint64
	types       []BalanceType

	// store id -> hot regions statistics as the role of leader
	stats *storeStatistics
	r     *rand.Rand
}

func newBalanceHotRegionsScheduler(opController *schedule.OperatorController) *balanceHotRegionsScheduler {
	base := newBaseScheduler(opController)
	return &balanceHotRegionsScheduler{
		name:          balanceHotRegionName,
		baseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotWriteRegionBalance, hotReadRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func newBalanceHotReadRegionsScheduler(opController *schedule.OperatorController) *balanceHotRegionsScheduler {
	base := newBaseScheduler(opController)
	return &balanceHotRegionsScheduler{
		baseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotReadRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func newBalanceHotWriteRegionsScheduler(opController *schedule.OperatorController) *balanceHotRegionsScheduler {
	base := newBaseScheduler(opController)
	return &balanceHotRegionsScheduler{
		baseScheduler: base,
		leaderLimit:   1,
		peerLimit:     1,
		stats:         newStoreStaticstics(),
		types:         []BalanceType{hotWriteRegionBalance},
		r:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (h *balanceHotRegionsScheduler) GetName() string {
	return h.name
}

func (h *balanceHotRegionsScheduler) GetType() string {
	return "hot-region"
}

func (h *balanceHotRegionsScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return h.allowBalanceLeader(cluster) || h.allowBalanceRegion(cluster)
}

func (h *balanceHotRegionsScheduler) allowBalanceLeader(cluster opt.Cluster) bool {
	return h.opController.OperatorCount(operator.OpHotRegion) < minUint64(h.leaderLimit, cluster.GetHotRegionScheduleLimit()) &&
		h.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (h *balanceHotRegionsScheduler) allowBalanceRegion(cluster opt.Cluster) bool {
	return h.opController.OperatorCount(operator.OpHotRegion) < minUint64(h.peerLimit, cluster.GetHotRegionScheduleLimit())
}

func (h *balanceHotRegionsScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(h.GetName(), "schedule").Inc()
	return h.dispatch(h.types[h.r.Int()%len(h.types)], cluster)
}

func (h *balanceHotRegionsScheduler) dispatch(typ BalanceType, cluster opt.Cluster) []*operator.Operator {
	h.Lock()
	defer h.Unlock()
	storesStat := cluster.GetStoresStats()
	switch typ {
	case hotReadRegionBalance:
		h.stats.readStatAsLeader = calcScore(cluster.RegionReadStats(), storesStat.GetStoresBytesReadStat(), cluster, core.LeaderKind)
		return h.balanceHotReadRegions(cluster)
	case hotWriteRegionBalance:
		h.stats.writeStatAsLeader = calcScore(cluster.RegionWriteStats(), storesStat.GetStoresBytesWriteStat(), cluster, core.LeaderKind)
		h.stats.writeStatAsPeer = calcScore(cluster.RegionWriteStats(), storesStat.GetStoresBytesWriteStat(), cluster, core.RegionKind)
		return h.balanceHotWriteRegions(cluster)
	}
	return nil
}

func (h *balanceHotRegionsScheduler) balanceHotReadRegions(cluster opt.Cluster) []*operator.Operator {
	// balance by leader
	srcRegion, newLeader := h.balanceByLeader(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		schedulerCounter.WithLabelValues(h.GetName(), "move-leader").Inc()
		op := operator.CreateTransferLeaderOperator("transfer-hot-read-leader", srcRegion, srcRegion.GetLeader().GetStoreId(), newLeader.GetStoreId(), operator.OpHotRegion)
		op.SetPriorityLevel(core.HighPriority)
		return []*operator.Operator{op}
	}

	// balance by peer
	srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster, h.stats.readStatAsLeader)
	if srcRegion != nil {
		op, err := operator.CreateMovePeerOperator("move-hot-read-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), destPeer.GetStoreId(), destPeer.GetId())
		if err != nil {
			schedulerCounter.WithLabelValues(h.GetName(), "create-operator-fail").Inc()
			return nil
		}
		op.SetPriorityLevel(core.HighPriority)
		schedulerCounter.WithLabelValues(h.GetName(), "move-peer").Inc()
		return []*operator.Operator{op}
	}
	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

// balanceHotRetryLimit is the limit to retry schedule for selected balance strategy.
const balanceHotRetryLimit = 10

func (h *balanceHotRegionsScheduler) balanceHotWriteRegions(cluster opt.Cluster) []*operator.Operator {
	for i := 0; i < balanceHotRetryLimit; i++ {
		switch h.r.Int() % 2 {
		case 0:
			// balance by peer
			srcRegion, srcPeer, destPeer := h.balanceByPeer(cluster, h.stats.writeStatAsPeer)
			if srcRegion != nil {
				op, err := operator.CreateMovePeerOperator("move-hot-write-region", cluster, srcRegion, operator.OpHotRegion, srcPeer.GetStoreId(), destPeer.GetStoreId(), destPeer.GetId())
				if err != nil {
					schedulerCounter.WithLabelValues(h.GetName(), "create-operator-fail").Inc()
					return nil
				}
				op.SetPriorityLevel(core.HighPriority)
				schedulerCounter.WithLabelValues(h.GetName(), "move-peer").Inc()
				return []*operator.Operator{op}
			}
		case 1:
			// balance by leader
			srcRegion, newLeader := h.balanceByLeader(cluster, h.stats.writeStatAsLeader)
			if srcRegion != nil {
				schedulerCounter.WithLabelValues(h.GetName(), "move-leader").Inc()
				op := operator.CreateTransferLeaderOperator("transfer-hot-write-leader", srcRegion, srcRegion.GetLeader().GetStoreId(), newLeader.GetStoreId(), operator.OpHotRegion)
				op.SetPriorityLevel(core.HighPriority)
				return []*operator.Operator{op}
			}
		}
	}

	schedulerCounter.WithLabelValues(h.GetName(), "skip").Inc()
	return nil
}

func calcScore(storeHotPeers map[uint64][]*statistics.HotPeerStat, storeBytesStat map[uint64]float64, cluster opt.Cluster, kind core.ResourceKind) statistics.StoreHotPeersStat {
	stats := make(statistics.StoreHotPeersStat)
	for storeID, items := range storeHotPeers {
		hotPeers, ok := stats[storeID]
		if !ok {
			hotPeers = &statistics.HotPeersStat{
				Stats: make([]statistics.HotPeerStat, 0, storeHotPeersDefaultLen),
			}
			stats[storeID] = hotPeers
		}

		for _, r := range items {
			if kind == core.LeaderKind && !r.IsLeader() {
				continue
			}
			// HotDegree is the update times on the hot cache. If the heartbeat report
			// the flow of the region exceeds the threshold, the scheduler will update the region in
			// the hot cache and the hotdegree of the region will increase.
			if r.HotDegree < cluster.GetHotRegionCacheHitsThreshold() {
				continue
			}

			regionInfo := cluster.GetRegion(r.RegionID)
			if regionInfo == nil {
				continue
			}

			s := statistics.HotPeerStat{
				StoreID:        storeID,
				RegionID:       r.RegionID,
				HotDegree:      r.HotDegree,
				AntiCount:      r.AntiCount,
				BytesRate:      r.GetBytesRate(),
				LastUpdateTime: r.LastUpdateTime,
				Version:        r.Version,
			}
			hotPeers.TotalBytesRate += r.GetBytesRate()
			hotPeers.Count++
			hotPeers.Stats = append(hotPeers.Stats, s)
		}

		if rate, ok := storeBytesStat[storeID]; ok {
			hotPeers.StoreBytesRate = rate
		}
	}
	return stats
}

// balanceByPeer balances the peer distribution of hot regions.
func (h *balanceHotRegionsScheduler) balanceByPeer(cluster opt.Cluster, storesStat statistics.StoreHotPeersStat) (*core.RegionInfo, *metapb.Peer, *metapb.Peer) {
	if !h.allowBalanceRegion(cluster) {
		return nil, nil, nil
	}

	srcStoreID := h.selectSrcStore(storesStat)
	if srcStoreID == 0 {
		return nil, nil, nil
	}

	// get one source region and a target store.
	// For each region in the source store, we try to find the best target store;
	// If we can find a target store, then return from this method.
	stores := cluster.GetStores()
	var destStoreID uint64
	for _, i := range h.r.Perm(len(storesStat[srcStoreID].Stats)) {
		rs := storesStat[srcStoreID].Stats[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no-region").Inc()
			continue
		}

		if !opt.IsHealthyAllowPending(cluster, srcRegion) {
			schedulerCounter.WithLabelValues(h.GetName(), "unhealthy-replica").Inc()
			continue
		}

		if !opt.IsRegionReplicated(cluster, srcRegion) {
			log.Debug("region has abnormal replica count", zap.String("scheduler", h.GetName()), zap.Uint64("region-id", srcRegion.GetID()))
			schedulerCounter.WithLabelValues(h.GetName(), "abnormal-replica").Inc()
			continue
		}

		srcStore := cluster.GetStore(srcStoreID)
		if srcStore == nil {
			log.Error("failed to get the source store", zap.Uint64("store-id", srcStoreID))
		}
		filters := []filter.Filter{
			filter.StoreStateFilter{ActionScope: h.GetName(), MoveRegion: true},
			filter.NewExcludedFilter(h.GetName(), srcRegion.GetStoreIds(), srcRegion.GetStoreIds()),
			filter.NewDistinctScoreFilter(h.GetName(), cluster.GetLocationLabels(), cluster.GetRegionStores(srcRegion), srcStore),
		}
		candidateStoreIDs := make([]uint64, 0, len(stores))
		for _, store := range stores {
			if filter.Target(cluster, store, filters) {
				continue
			}
			candidateStoreIDs = append(candidateStoreIDs, store.GetID())
		}

		destStoreID = h.selectDestStore(candidateStoreIDs, rs.GetBytesRate(), srcStoreID, storesStat)
		if destStoreID != 0 {
			h.peerLimit = h.adjustBalanceLimit(srcStoreID, storesStat)

			srcPeer := srcRegion.GetStorePeer(srcStoreID)
			if srcPeer == nil {
				return nil, nil, nil
			}

			// When the target store is decided, we allocate a peer ID to hold the source region,
			// because it doesn't exist in the system right now.
			destPeer, err := cluster.AllocPeer(destStoreID)
			if err != nil {
				log.Error("failed to allocate peer", zap.Error(err))
				return nil, nil, nil
			}

			return srcRegion, srcPeer, destPeer
		}
	}

	return nil, nil, nil
}

// balanceByLeader balances the leader distribution of hot regions.
func (h *balanceHotRegionsScheduler) balanceByLeader(cluster opt.Cluster, storesStat statistics.StoreHotPeersStat) (*core.RegionInfo, *metapb.Peer) {
	if !h.allowBalanceLeader(cluster) {
		return nil, nil
	}

	srcStoreID := h.selectSrcStore(storesStat)
	if srcStoreID == 0 {
		return nil, nil
	}

	// select destPeer
	for _, i := range h.r.Perm(len(storesStat[srcStoreID].Stats)) {
		rs := storesStat[srcStoreID].Stats[i]
		srcRegion := cluster.GetRegion(rs.RegionID)
		if srcRegion == nil {
			schedulerCounter.WithLabelValues(h.GetName(), "no-region").Inc()
			continue
		}

		if !opt.IsHealthyAllowPending(cluster, srcRegion) {
			schedulerCounter.WithLabelValues(h.GetName(), "unhealthy-replica").Inc()
			continue
		}

		filters := []filter.Filter{filter.StoreStateFilter{ActionScope: h.GetName(), TransferLeader: true}}
		candidateStoreIDs := make([]uint64, 0, len(srcRegion.GetPeers())-1)
		for _, store := range cluster.GetFollowerStores(srcRegion) {
			if !filter.Target(cluster, store, filters) {
				candidateStoreIDs = append(candidateStoreIDs, store.GetID())
			}
		}
		if len(candidateStoreIDs) == 0 {
			continue
		}
		destStoreID := h.selectDestStore(candidateStoreIDs, rs.GetBytesRate(), srcStoreID, storesStat)
		if destStoreID == 0 {
			continue
		}

		destPeer := srcRegion.GetStoreVoter(destStoreID)
		if destPeer != nil {
			h.leaderLimit = h.adjustBalanceLimit(srcStoreID, storesStat)

			return srcRegion, destPeer
		}
	}
	return nil, nil
}

// Select the store to move hot regions from.
// We choose the store with the maximum number of hot region first.
// Inside these stores, we choose the one with maximum flow bytes.
func (h *balanceHotRegionsScheduler) selectSrcStore(stats statistics.StoreHotPeersStat) (srcStoreID uint64) {
	var (
		maxFlowBytes float64
		maxCount     int
	)

	for storeID, stat := range stats {
		count, flowBytes := len(stat.Stats), stat.StoreBytesRate
		if count <= 1 {
			continue
		}
		if flowBytes > maxFlowBytes || (flowBytes == maxFlowBytes && count > maxCount) {
			maxCount = count
			maxFlowBytes = flowBytes
			srcStoreID = storeID
		}
	}
	return
}

// selectDestStore selects a target store to hold the region of the source region.
// We choose a target store based on the hot region number and flow bytes of this store.
func (h *balanceHotRegionsScheduler) selectDestStore(candidateStoreIDs []uint64, regionBytesRate float64, srcStoreID uint64, storesStat statistics.StoreHotPeersStat) (destStoreID uint64) {
	srcBytesRate := storesStat[srcStoreID].StoreBytesRate

	var (
		minBytesRate float64 = srcBytesRate*hotRegionScheduleFactor - regionBytesRate
		minCount             = int(math.MaxInt32)
	)
	for _, storeID := range candidateStoreIDs {
		if s, ok := storesStat[storeID]; ok {
			count, dstBytesRate := len(s.Stats), s.StoreBytesRate
			if minBytesRate > dstBytesRate || (minBytesRate == dstBytesRate && minCount > count) {
				minCount = count
				minBytesRate = dstBytesRate
				destStoreID = storeID
			}
		} else {
			destStoreID = storeID
			return
		}
	}
	return
}

func (h *balanceHotRegionsScheduler) adjustBalanceLimit(storeID uint64, storesStat statistics.StoreHotPeersStat) uint64 {
	srcStoreStatistics := storesStat[storeID]

	var hotRegionTotalCount int
	for _, m := range storesStat {
		hotRegionTotalCount += len(m.Stats)
	}

	avgRegionCount := float64(hotRegionTotalCount) / float64(len(storesStat))
	// Multiplied by hotRegionLimitFactor to avoid transfer back and forth
	limit := uint64((float64(len(srcStoreStatistics.Stats)) - avgRegionCount) * hotRegionLimitFactor)
	return maxUint64(limit, 1)
}

func (h *balanceHotRegionsScheduler) GetHotReadStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stats.readStatAsLeader))
	for id, stat := range h.stats.readStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
	}
}

func (h *balanceHotRegionsScheduler) GetHotWriteStatus() *statistics.StoreHotPeersInfos {
	h.RLock()
	defer h.RUnlock()
	asLeader := make(statistics.StoreHotPeersStat, len(h.stats.writeStatAsLeader))
	asPeer := make(statistics.StoreHotPeersStat, len(h.stats.writeStatAsPeer))
	for id, stat := range h.stats.writeStatAsLeader {
		clone := *stat
		asLeader[id] = &clone
	}
	for id, stat := range h.stats.writeStatAsPeer {
		clone := *stat
		asPeer[id] = &clone
	}
	return &statistics.StoreHotPeersInfos{
		AsLeader: asLeader,
		AsPeer:   asPeer,
	}
}
