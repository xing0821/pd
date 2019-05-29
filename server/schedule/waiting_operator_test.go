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

func (s *testWaitingOperatorSuite) TestRandBuckets(c *C) {
	rb := NewRandBuckets()
	addOperators(rb)
	for i := 0; i < 3; i++ {
		c.Assert(rb.GetOperator(), NotNil)
	}
	c.Assert(rb.GetOperator(), IsNil)
}

func addOperators(wop WaitingOperator) {
	op := NewOperator("testOperatorNormal", uint64(1), &metapb.RegionEpoch{}, OpRegion, []OperatorStep{
		RemovePeer{FromStore: uint64(1)},
	}...)
	wop.PutOperator(op)
	op = NewOperator("testOperatorHigh", uint64(2), &metapb.RegionEpoch{}, OpRegion, []OperatorStep{
		RemovePeer{FromStore: uint64(2)},
	}...)
	op.SetPriorityLevel(core.HighPriority)
	wop.PutOperator(op)
	op = NewOperator("testOperatorLow", uint64(3), &metapb.RegionEpoch{}, OpRegion, []OperatorStep{
		RemovePeer{FromStore: uint64(3)},
	}...)
	op.SetPriorityLevel(core.LowPriority)
	wop.PutOperator(op)
}
