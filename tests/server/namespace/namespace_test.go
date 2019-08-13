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

package namespace_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/checker"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedulers"
)

func TestNamespace(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testNamespaceSuite{})

type testNamespaceSuite struct {
	classifier *mapClassifer
	cluster    *mockcluster.Cluster
	opt        *mockoption.ScheduleOptions
}

func (s *testNamespaceSuite) SetUpTest(c *C) {
	s.classifier = newMapClassifer()
	s.opt = mockoption.NewScheduleOptions()
	s.cluster = mockcluster.NewCluster(s.opt)
}

func (s *testNamespaceSuite) TestReplica(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2          10       ns1
	//     3           0       ns2
	s.cluster.AddRegionStore(1, 0)
	s.cluster.AddRegionStore(2, 10)
	s.cluster.AddRegionStore(3, 0)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")

	rc := checker.NewReplicaChecker(s.cluster, s.classifier)

	// Replica should be added to the store with the same namespace.
	s.classifier.setRegion(1, "ns1")
	s.cluster.AddLeaderRegion(1, 1)
	op := rc.Check(s.cluster.GetRegion(1))
	testutil.CheckAddPeer(c, op, operator.OpReplica, 2)
	s.cluster.AddLeaderRegion(1, 3)
	op = rc.Check(s.cluster.GetRegion(1))
	testutil.CheckAddPeer(c, op, operator.OpReplica, 1)

	// Stop adding replica if no store in the same namespace.
	s.cluster.AddLeaderRegion(1, 1, 2)
	op = rc.Check(s.cluster.GetRegion(1))
	c.Assert(op, IsNil)
}

func (s *testNamespaceSuite) TestNamespaceChecker(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2          10       ns1
	//     3           0       ns2
	s.cluster.AddRegionStore(1, 0)
	s.cluster.AddRegionStore(2, 10)
	s.cluster.AddRegionStore(3, 0)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")

	nc := checker.NewNamespaceChecker(s.cluster, s.classifier)

	// Move the region if it was not in the right store.
	s.classifier.setRegion(1, "ns2")
	s.cluster.AddLeaderRegion(1, 1)
	op := nc.Check(s.cluster.GetRegion(1))
	testutil.CheckTransferPeer(c, op, operator.OpReplica, 1, 3)

	// Only move one region if the one was in the right store while the other was not.
	s.classifier.setRegion(2, "ns1")
	s.cluster.AddLeaderRegion(2, 1)
	s.classifier.setRegion(3, "ns2")
	s.cluster.AddLeaderRegion(3, 2)
	op = nc.Check(s.cluster.GetRegion(2))
	c.Assert(op, IsNil)
	op = nc.Check(s.cluster.GetRegion(3))
	testutil.CheckTransferPeer(c, op, operator.OpReplica, 2, 3)

	// Do NOT move the region if it was in the right store.
	s.classifier.setRegion(4, "ns2")
	s.cluster.AddLeaderRegion(4, 3)
	op = nc.Check(s.cluster.GetRegion(4))
	c.Assert(op, IsNil)

	// Move the peer with questions to the right store if the region has multiple peers.
	s.classifier.setRegion(5, "ns1")
	s.cluster.AddLeaderRegion(5, 1, 1, 3)

	s.opt.DisableNamespaceRelocation = true
	c.Assert(nc.Check(s.cluster.GetRegion(5)), IsNil)
	s.opt.DisableNamespaceRelocation = false

	op = nc.Check(s.cluster.GetRegion(5))
	testutil.CheckTransferPeer(c, op, operator.OpReplica, 3, 2)
}

func (s *testNamespaceSuite) TestSchedulerBalanceRegion(c *C) {
	// store regionCount namespace
	//     1           0       ns1
	//     2         100       ns1
	//     3         200       ns2
	s.cluster.AddRegionStore(1, 0)
	s.cluster.AddRegionStore(2, 100)
	s.cluster.AddRegionStore(3, 200)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")
	s.opt.SetMaxReplicas(1)

	oc := schedule.NewOperatorController(nil, nil)
	sched, _ := schedulers.CreateScheduler("balance-region", oc)

	// Balance is limited within a namespace.
	s.cluster.AddLeaderRegion(1, 2)
	s.classifier.setRegion(1, "ns1")
	op := schedulers.ScheduleByNamespace(s.cluster, s.classifier, sched)
	testutil.CheckTransferPeer(c, op[0], operator.OpBalance, 2, 1)

	// If no more store in the namespace, balance stops.
	s.cluster.AddLeaderRegion(1, 3)
	s.classifier.setRegion(1, "ns2")
	op = schedulers.ScheduleByNamespace(s.cluster, s.classifier, sched)
	c.Assert(op, IsNil)

	// If region is not in the correct namespace, it will not be balanced. The
	// region should be in 'ns1', but its replica is located in 'ns2', neither
	// namespace will select it for balance.
	s.cluster.AddRegionStore(4, 0)
	s.classifier.setStore(4, "ns2")
	s.cluster.AddLeaderRegion(1, 3)
	s.classifier.setRegion(1, "ns1")
	op = schedulers.ScheduleByNamespace(s.cluster, s.classifier, sched)
	c.Assert(op, IsNil)
}

func (s *testNamespaceSuite) TestSchedulerBalanceLeader(c *C) {
	// store regionCount namespace
	//     1         100       ns1
	//     2         200       ns1
	//     3           0       ns2
	//     4         300       ns2
	s.cluster.AddLeaderStore(1, 100)
	s.cluster.AddLeaderStore(2, 200)
	s.cluster.AddLeaderStore(3, 0)
	s.cluster.AddLeaderStore(4, 300)
	s.classifier.setStore(1, "ns1")
	s.classifier.setStore(2, "ns1")
	s.classifier.setStore(3, "ns2")
	s.classifier.setStore(4, "ns2")

	oc := schedule.NewOperatorController(nil, nil)
	sched, _ := schedulers.CreateScheduler("balance-leader", oc)

	// Balance is limited within a namespace.
	s.cluster.AddLeaderRegion(1, 2, 1)
	s.classifier.setRegion(1, "ns1")
	op := schedulers.ScheduleByNamespace(s.cluster, s.classifier, sched)
	testutil.CheckTransferLeader(c, op[0], operator.OpBalance, 2, 1)

	// If region is not in the correct namespace, it will not be balanced.
	s.cluster.AddLeaderRegion(1, 4, 1)
	s.classifier.setRegion(1, "ns1")
	op = schedulers.ScheduleByNamespace(s.cluster, s.classifier, sched)
	c.Assert(op, IsNil)
}

type mapClassifer struct {
	stores  map[uint64]string
	regions map[uint64]string
}

func newMapClassifer() *mapClassifer {
	return &mapClassifer{
		stores:  make(map[uint64]string),
		regions: make(map[uint64]string),
	}
}

func (c *mapClassifer) GetStoreNamespace(store *core.StoreInfo) string {
	if ns, ok := c.stores[store.GetID()]; ok {
		return ns
	}
	return namespace.DefaultNamespace
}

func (c *mapClassifer) GetRegionNamespace(region *core.RegionInfo) string {
	if ns, ok := c.regions[region.GetID()]; ok {
		return ns
	}
	return namespace.DefaultNamespace
}

func (c *mapClassifer) GetAllNamespaces() []string {
	all := make(map[string]struct{})
	for _, ns := range c.stores {
		all[ns] = struct{}{}
	}
	for _, ns := range c.regions {
		all[ns] = struct{}{}
	}

	nss := make([]string, 0, len(all))

	for ns := range all {
		nss = append(nss, ns)
	}
	return nss
}

func (c *mapClassifer) IsNamespaceExist(name string) bool {
	for _, ns := range c.stores {
		if ns == name {
			return true
		}
	}
	for _, ns := range c.regions {
		if ns == name {
			return true
		}
	}
	return false
}

func (c *mapClassifer) IsMetaExist() bool {
	return false
}

func (c *mapClassifer) IsTableIDExist(tableID int64) bool {
	return false
}

func (c *mapClassifer) IsStoreIDExist(storeID uint64) bool {
	return false
}

func (c *mapClassifer) AllowMerge(one *core.RegionInfo, other *core.RegionInfo) bool {
	return c.GetRegionNamespace(one) == c.GetRegionNamespace(other)
}

func (c *mapClassifer) ReloadNamespaces() error {
	return nil
}

func (c *mapClassifer) setStore(id uint64, namespace string) {
	c.stores[id] = namespace
}

func (c *mapClassifer) setRegion(id uint64, namespace string) {
	c.regions[id] = namespace
}
