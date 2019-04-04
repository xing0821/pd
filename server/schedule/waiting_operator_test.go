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

package schedule

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testWaitingOperatorSuite{})

type testWaitingOperatorSuite struct{}

func (s *testWaitingOperatorSuite) TestPriorityQueue(c *C) {
	pq := NewPriorityQueue()
	addOperators(pq)
	res := []uint64{2, 1, 3, 4}
	for i := 0; i < 4; i++ {
		c.Assert(pq.GetOperator().regionID, Equals, res[i])
	}
}

func (s *testWaitingOperatorSuite) TestRandQueue(c *C) {
	rq := NewRandQueue()
	addOperators(rq)
	for i := 0; i < 4; i++ {
		c.Assert(rq.GetOperator(), NotNil)
	}
}

func (s *testWaitingOperatorSuite) TestRandBuckets(c *C) {
	rb := NewRandBuckets()
	addOperators(rb)
	for i := 0; i < 4; i++ {
		c.Assert(rb.GetOperator(), NotNil)
	}
}

func addOperators(wop WaitingOperator) {
	for i := 1; i <= 4; i++ {
		var op *Operator
		if i == 2 {
			op = NewOperator("testOperator1", uint64(i), &metapb.RegionEpoch{}, OpRegion, []OperatorStep{
				RemovePeer{FromStore: uint64(i)},
			}...)
			op.SetPriorityLevel(core.HighPriority)
		} else {
			op = NewOperator("testOperator2", uint64(i), &metapb.RegionEpoch{}, OpRegion, []OperatorStep{
				RemovePeer{FromStore: uint64(i)},
			}...)
		}
		wop.PutOperator(op)
	}
}
