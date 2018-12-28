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

package schedule

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// Simulating is an option to overpass the impact of accelerated time. Should
// only turned on by the simulator.
var Simulating bool

// Options for schedulers.
type Options interface {
	GetMaxBalanceLeaderInflight() uint64
	GetMaxBalanceRegionInflight() uint64
	GetMaxMakeupReplicaInflight() uint64
	GetMaxMergeRegionInflight() uint64
	GetMaxMakeNamespaceRelocationInflight() uint64
	GetMaxEvictLeaderInflight() uint64
	GetMaxGrantLeaderInflight() uint64
	GetMaxHotLeaderInflight() uint64
	GetMaxHotRegionInflight() uint64
	GetMaxLabelRejectLeaderInflight() uint64
	GetMaxRandomMergeInflight() uint64
	GetMaxScatterRangeInflight() uint64
	GetMaxShuffleLeaderInflight() uint64
	GetMaxShuffleRegionInflight() uint64
	GetMaxShuffleHotRegionInflight() uint64

	GetMaxSnapshotCount() uint64
	GetMaxPendingPeerCount() uint64
	GetMaxStoreDownTime() time.Duration
	GetMaxMergeRegionSize() uint64
	GetMaxMergeRegionKeys() uint64
	GetSplitMergeInterval() time.Duration

	GetMaxReplicas() int
	GetLocationLabels() []string

	GetHotRegionLowThreshold() int
	GetTolerantSizeRatio() float64
	GetLowSpaceRatio() float64
	GetHighSpaceRatio() float64

	IsRaftLearnerEnabled() bool

	IsRemoveDownReplicaEnabled() bool
	IsReplaceOfflineReplicaEnabled() bool
	IsMakeUpReplicaEnabled() bool
	IsRemoveExtraReplicaEnabled() bool
	IsLocationReplacementEnabled() bool
	IsNamespaceRelocationEnabled() bool

	CheckLabelProperty(typ string, labels []*metapb.StoreLabel) bool
}

// NamespaceOptions for namespace cluster.
type NamespaceOptions interface {
	GetMaxBalanceLeaderInflight(name string) uint64
	GetMaxBalanceRegionInflight(name string) uint64
	GetMaxMakeupReplicaInflight(name string) uint64
	GetMaxMergeRegionInflight(name string) uint64
	GetMaxMakeNamespaceRelocationInflight(name string) uint64
	GetMaxEvictLeaderInflight(name string) uint64
	GetMaxGrantLeaderInflight(name string) uint64
	GetMaxHotLeaderInflight(name string) uint64
	GetMaxHotRegionInflight(name string) uint64
	GetMaxLabelRejectLeaderInflight(name string) uint64
	GetMaxRandomMergeInflight(name string) uint64
	GetMaxScatterRangeInflight(name string) uint64
	GetMaxShuffleLeaderInflight(name string) uint64
	GetMaxShuffleRegionInflight(name string) uint64
	GetMaxShuffleHotRegionInflight(name string) uint64
	GetMaxReplicas(name string) int
}

const (
	// RejectLeader is the label property type that sugguests a store should not
	// have any region leaders.
	RejectLeader = "reject-leader"
)
