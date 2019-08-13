// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package namespace

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/statistics"
)

// namespaceCluster provides an overview of a cluster's regions distribution.
type namespaceCluster interface {
	core.RegionSetInformer
	core.StoreSetInformer
	core.StoreSetController

	statistics.RegionStatInformer
	opt.Options

	// get config methods
	GetOpt() ScheduleOptions
	// TODO: it should be removed. Schedulers don't need to know anything
	// about peers.
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}

// ScheduleOptions for namespace cluster.
type ScheduleOptions interface {
	GetLeaderScheduleLimit(name string) uint64
	GetRegionScheduleLimit(name string) uint64
	GetReplicaScheduleLimit(name string) uint64
	GetMergeScheduleLimit(name string) uint64
	GetMaxReplicas(name string) int
}

// Classifier is used to determine the namespace which the store or region
// belongs.
type Classifier interface {
	GetAllNamespaces() []string
	GetStoreNamespace(*core.StoreInfo) string
	GetRegionNamespace(*core.RegionInfo) string
	IsNamespaceExist(name string) bool
	AllowMerge(*core.RegionInfo, *core.RegionInfo) bool
	// Reload underlying namespaces
	ReloadNamespaces() error
	// These function below are only for tests
	IsMetaExist() bool
	IsTableIDExist(int64) bool
	IsStoreIDExist(uint64) bool
}
