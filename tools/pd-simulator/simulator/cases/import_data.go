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
	"bytes"
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/table"
	"github.com/pingcap/pd/tools/pd-simulator/simulator/simutil"
)

func newImportData() *Case {
	var simCase Case
	var id idAllocator
	// Initialize the cluster
	for i := 1; i <= 10; i++ {
		simCase.Stores = append(simCase.Stores, &Store{
			ID:        id.nextID(),
			Status:    metapb.StoreState_Up,
			Capacity:  1 * TB,
			Available: 900 * GB,
			Version:   "2.1.0",
		})
	}

	for i := 0; i < 30; i++ {
		storeIDs := rand.Perm(10)
		peers := []*metapb.Peer{
			{Id: id.nextID(), StoreId: uint64(storeIDs[0] + 1)},
			{Id: id.nextID(), StoreId: uint64(storeIDs[1] + 1)},
			{Id: id.nextID(), StoreId: uint64(storeIDs[2] + 1)},
		}
		simCase.Regions = append(simCase.Regions, Region{
			ID:     id.nextID(),
			Peers:  peers,
			Leader: peers[0],
			Size:   32 * MB,
			Keys:   320000,
		})
	}
	simCase.MaxID = id.maxID
	simCase.RegionSplitSize = 64 * MB
	simCase.RegionSplitKeys = 640000
	simCase.TableNumber = 10
	// Events description
	e := &WriteFlowOnSpotInner{}
	table2 := string(table.EncodeBytes(table.GenerateTableKey(2)))
	table3 := string(table.EncodeBytes(table.GenerateTableKey(3)))
	table5 := string(table.EncodeBytes(table.GenerateTableKey(5)))
	e.Step = func(tick int64) map[string]int64 {
		return map[string]int64{
			table2: 4 * MB,
			table3: 1 * MB,
			table5: 16 * MB,
		}
	}
	simCase.Events = []EventInner{e}

	// Checker description
	startTime := time.Now()
	simCase.Checker = func(regions *core.RegionsInfo) bool {
		leaderDstb := make(map[uint64]int)
		peerDstb := make(map[uint64]int)
		leaderTotal := 0
		peerTotal := 0
		res := make([]*core.RegionInfo, 0, 100)
		regions.ScanRangeWithIterator([]byte(table2), func(region *metapb.Region) bool {
			if bytes.Compare(region.EndKey, []byte(table3)) < 0 {
				res = append(res, regions.GetRegion(region.GetId()))
				return true
			}
			return false
		})

		for _, r := range res {
			leaderDstb[r.GetLeader().GetStoreId()]++
			leaderTotal++
			for _, p := range r.GetPeers() {
				peerDstb[p.GetStoreId()]++
				peerTotal++
			}
		}
		if leaderTotal == 0 || peerTotal == 0 {
			return false
		}
		leaderLog := "leader distribute (table2)"
		peerLog := "peer distribute (table2)"
		for storeID := 1; storeID <= 10; storeID++ {
			if leaderCount, ok := leaderDstb[uint64(storeID)]; ok {
				leaderLog = fmt.Sprintf("%s [store %d]:%.2f%%", leaderLog, storeID, float64(leaderCount)/float64(leaderTotal)*100)
			}
		}
		for storeID := 1; storeID <= 10; storeID++ {
			if peerCount, ok := peerDstb[uint64(storeID)]; ok {
				peerLog = fmt.Sprintf("%s [store %d]:%.2f%%", peerLog, storeID, float64(peerCount)/float64(peerTotal)*100)
			}
		}

		simutil.Logger.Info(leaderLog)
		simutil.Logger.Info(peerLog)
		return startTime.Add(time.Minute).Before(time.Now())
	}
	return &simCase
}
