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

package checker

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
)

func TestReplicaChecker(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testReplicaCheckerSuite{})

type testReplicaCheckerSuite struct {
	cluster *mockcluster.Cluster
	rc      *ReplicaChecker
}

func (s *testReplicaCheckerSuite) SetUpTest(c *C) {
	cfg := mockoption.NewScheduleOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.rc = NewReplicaChecker(s.cluster, namespace.DefaultClassifier)
}

func (s *testReplicaCheckerSuite) TestReplacePendingPeer(c *C) {
	stats := &pdpb.StoreStats{
		Capacity:  100,
		Available: 100,
	}
	stores := []*core.StoreInfo{
		core.NewStoreInfo(
			&metapb.Store{
				Id:    1,
				State: metapb.StoreState_Offline,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id:    2,
				State: metapb.StoreState_Up,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id:    3,
				State: metapb.StoreState_Up,
			},
			core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
		core.NewStoreInfo(
			&metapb.Store{
				Id:    4,
				State: metapb.StoreState_Up,
			}, core.SetStoreStats(stats),
			core.SetLastHeartbeatTS(time.Now()),
		),
	}
	for _, store := range stores {
		s.cluster.PutStore(store)
	}
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      4,
			StoreId: 3,
		},
	}
	r := core.NewRegionInfo(&metapb.Region{Id: 1, Peers: peers}, peers[1], core.WithPendingPeers(peers[0:1]))
	s.cluster.PutRegion(r)
	op := s.rc.Check(r)
	c.Assert(op, NotNil)
	c.Assert(op.Step(0).(schedule.AddLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(1).(schedule.PromoteLearner).ToStore, Equals, uint64(4))
	c.Assert(op.Step(2).(schedule.RemovePeer).FromStore, Equals, uint64(1))
}
