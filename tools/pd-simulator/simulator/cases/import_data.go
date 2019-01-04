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

package cases

import (
	"math/rand"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/table"
)

func newImportData() *Case {
	var simCase Case
	// Initialize the cluster
	for i := 1; i <= 10; i++ {
		IDAllocator.nextID()
	}
	for i := 1; i <= 150; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        IDAllocator.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  8 * TB,
			Available: 8 * TB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 2000000; i++ {
		storeIDs := rand.Perm(150)
		peers := []*metapb.Peer{
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[0] + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[1] + 1)},
			{Id: IDAllocator.nextID(), StoreId: uint64(storeIDs[2] + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     IDAllocator.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   32 * MB,
			Keys:   320000,
		})
	}

	simCase.RegionSplitSize = 96 * MB
	simCase.RegionSplitKeys = 960000
	simCase.TableNumber = 150
	// Events description
	e := &WriteFlowOnSpotDescriptor{}
	e.Step = func(tick int64) map[string]int64 {
		flow := make(map[string]int64)
		for i := 0; i < 150; i++ {
			flow[string(table.EncodeBytes(table.GenerateTableKey(int64(i))))] = 2 * MB
		}
		return flow
	}
	simCase.Events = []EventDescriptor{e}

	// Checker description
	simCase.Checker = func(regions *core.RegionsInfo) bool {
		return false
	}
	return &simCase
}
