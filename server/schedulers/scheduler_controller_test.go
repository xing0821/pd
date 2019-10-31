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

package schedulers

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/mock/mockcluster"
	"github.com/pingcap/pd/pkg/mock/mockhbstream"
	"github.com/pingcap/pd/pkg/mock/mockoption"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/kv"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
)

var _ = Suite(&testScheduleControllerSuite{})

type testScheduleControllerSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testScheduleControllerSuite) SetUpSuite(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testScheduleControllerSuite) TearDownSuite(c *C) {
	s.cancel()
}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedule.Scheduler
	limit   uint64
	counter *schedule.OperatorController
	kind    operator.OpKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func (s *testScheduleControllerSuite) TestController(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(ctx, tc, mockhbstream.NewHeartbeatStream())

	tc.AddLeaderRegion(1, 1)
	tc.AddLeaderRegion(2, 2)

	scheduler, err := schedule.CreateScheduler("balance-leader", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := NewSchedulerController(ctx, tc, oc, lb)

	for i := MinScheduleInterval; sc.GetInterval() != MaxScheduleInterval; i = sc.GetNextInterval(i) {
		c.Assert(sc.GetInterval(), Equals, i)
		c.Assert(sc.Schedule(), IsNil)
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	c.Assert(sc.AllowSchedule(), IsTrue)
	op1 := testutil.NewTestOperator(1, tc.GetRegion(1).GetRegionEpoch(), operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op1), IsTrue)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)
	op2 := testutil.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op2), IsTrue)
	// count = 2
	c.Assert(sc.AllowSchedule(), IsFalse)
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	// count = 1
	c.Assert(sc.AllowSchedule(), IsTrue)

	// add a PriorityKind operator will remove old operator
	op3 := testutil.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpHotRegion)
	op3.SetPriorityLevel(core.HighPriority)
	c.Assert(oc.AddWaitingOperator(op1), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	c.Assert(oc.AddWaitingOperator(op3), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
	c.Assert(oc.RemoveOperator(op3), IsTrue)

	// add a admin operator will remove old operator
	c.Assert(oc.AddWaitingOperator(op2), IsTrue)
	c.Assert(sc.AllowSchedule(), IsFalse)
	op4 := testutil.NewTestOperator(2, tc.GetRegion(2).GetRegionEpoch(), operator.OpAdmin)
	op4.SetPriorityLevel(core.HighPriority)
	c.Assert(oc.AddWaitingOperator(op4), IsTrue)
	c.Assert(sc.AllowSchedule(), IsTrue)
	c.Assert(oc.RemoveOperator(op4), IsTrue)

	// test wrong region id.
	op5 := testutil.NewTestOperator(3, &metapb.RegionEpoch{}, operator.OpHotRegion)
	c.Assert(oc.AddWaitingOperator(op5), IsFalse)

	// test wrong region epoch.
	c.Assert(oc.RemoveOperator(op1), IsTrue)
	epoch := &metapb.RegionEpoch{
		Version: tc.GetRegion(1).GetRegionEpoch().GetVersion() + 1,
		ConfVer: tc.GetRegion(1).GetRegionEpoch().GetConfVer(),
	}
	op6 := testutil.NewTestOperator(1, epoch, operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op6), IsFalse)
	epoch.Version--
	op6 = testutil.NewTestOperator(1, epoch, operator.OpLeader)
	c.Assert(oc.AddWaitingOperator(op6), IsTrue)
	c.Assert(oc.RemoveOperator(op6), IsTrue)
}

func (s *testScheduleControllerSuite) TestInterval(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := mockoption.NewScheduleOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(ctx, tc, mockhbstream.NewHeartbeatStream())

	lb, err := schedule.CreateScheduler("balance-leader", oc, core.NewStorage(kv.NewMemoryKV()), nil)
	c.Assert(err, IsNil)
	sc := NewSchedulerController(ctx, tc, oc, lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.nextInterval = MinScheduleInterval
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			c.Assert(sc.Schedule(), IsNil)
		}
		c.Assert(sc.GetInterval(), Less, time.Second*time.Duration(n/2))
	}
}
