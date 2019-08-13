// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/statistics"
)

// Cluster is part of a global cluster that contains stores and regions
// within a specific namespace.
type Cluster struct {
	namespaceCluster
	classifier Classifier
	namespace  string
	stores     map[uint64]*core.StoreInfo
}

// NewCluster creates a new cluster according to the namespace.
func NewCluster(c namespaceCluster, classifier Classifier, namespace string) *Cluster {
	stores := make(map[uint64]*core.StoreInfo)
	for _, s := range c.GetStores() {
		if classifier.GetStoreNamespace(s) == namespace {
			stores[s.GetID()] = s
		}
	}
	return &Cluster{
		namespaceCluster: c,
		classifier:       classifier,
		namespace:        namespace,
		stores:           stores,
	}
}

func (c *Cluster) checkRegion(region *core.RegionInfo) bool {
	if c.classifier.GetRegionNamespace(region) != c.namespace {
		return false
	}
	for _, p := range region.GetPeers() {
		if _, ok := c.stores[p.GetStoreId()]; !ok {
			return false
		}
	}
	return true
}

const randRegionMaxRetry = 10

// RandFollowerRegion returns a random region that has a follower on the store.
func (c *Cluster) RandFollowerRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	for i := 0; i < randRegionMaxRetry; i++ {
		r := c.namespaceCluster.RandFollowerRegion(storeID, opts...)
		if r == nil {
			return nil
		}
		if c.checkRegion(r) {
			return r
		}
	}
	return nil
}

// RandLeaderRegion returns a random region that has leader on the store.
func (c *Cluster) RandLeaderRegion(storeID uint64, opts ...core.RegionOption) *core.RegionInfo {
	for i := 0; i < randRegionMaxRetry; i++ {
		r := c.namespaceCluster.RandLeaderRegion(storeID, opts...)
		if r == nil {
			return nil
		}
		if c.checkRegion(r) {
			return r
		}
	}
	return nil
}

// GetAverageRegionSize returns the average region approximate size.
func (c *Cluster) GetAverageRegionSize() int64 {
	var totalCount, totalSize int64
	for _, s := range c.stores {
		totalCount += int64(s.GetRegionCount())
		totalSize += s.GetRegionSize()
	}
	if totalCount == 0 {
		return 0
	}
	return totalSize / totalCount
}

// GetStores returns all stores in the namespace.
func (c *Cluster) GetStores() []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, len(c.stores))
	for _, s := range c.stores {
		stores = append(stores, s)
	}
	return stores
}

// GetStore searches for a store by ID.
func (c *Cluster) GetStore(id uint64) *core.StoreInfo {
	return c.stores[id]
}

// GetRegion searches for a region by ID.
func (c *Cluster) GetRegion(id uint64) *core.RegionInfo {
	r := c.namespaceCluster.GetRegion(id)
	if r == nil || !c.checkRegion(r) {
		return nil
	}
	return r
}

// RegionWriteStats returns hot region's write stats.
func (c *Cluster) RegionWriteStats() map[uint64][]*statistics.HotSpotPeerStat {
	return c.namespaceCluster.RegionWriteStats()
}

// GetLeaderScheduleLimit returns the limit for leader schedule.
func (c *Cluster) GetLeaderScheduleLimit() uint64 {
	return c.GetOpt().GetLeaderScheduleLimit(c.namespace)
}

// GetRegionScheduleLimit returns the limit for region schedule.
func (c *Cluster) GetRegionScheduleLimit() uint64 {
	return c.GetOpt().GetRegionScheduleLimit(c.namespace)
}

// GetReplicaScheduleLimit returns the limit for replica schedule.
func (c *Cluster) GetReplicaScheduleLimit() uint64 {
	return c.GetOpt().GetReplicaScheduleLimit(c.namespace)
}

// GetMergeScheduleLimit returns the limit for merge schedule.
func (c *Cluster) GetMergeScheduleLimit() uint64 {
	return c.GetOpt().GetMergeScheduleLimit(c.namespace)
}

// GetMaxReplicas returns the number of replicas.
func (c *Cluster) GetMaxReplicas() int {
	return c.GetOpt().GetMaxReplicas(c.namespace)
}
