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

package faketikv

import (
	"math/rand"
	"sort"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/faketikv/cases"
	"github.com/pingcap/pd/pkg/faketikv/simutil"
	"github.com/pingcap/pd/server/core"
)

// RaftInfo records all raft infomation.
type RaftInfo struct {
	*core.RegionsInfo
	conn         *Conn
	regionchange map[uint64][]uint64
}

// NewRaftInfo creates the initialized raft with the configuration.
func NewRaftInfo(conf *cases.Conf, conn *Conn) (*RaftInfo, error) {
	r := &RaftInfo{
		RegionsInfo:  core.NewRegionsInfo(),
		conn:         conn,
		regionchange: make(map[uint64][]uint64),
	}

	splitKeys := generateKeys(len(conf.Regions) - 1)
	for i, region := range conf.Regions {
		meta := &metapb.Region{
			Id:          region.ID,
			Peers:       region.Peers,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		}
		if i > 0 {
			meta.StartKey = []byte(splitKeys[i-1])
		}
		if i < len(conf.Regions)-1 {
			meta.EndKey = []byte(splitKeys[i])
		}
		regionInfo := core.NewRegionInfo(meta, region.Leader)
		regionInfo.ApproximateSize = region.Size
		regionInfo.ApproximateKeys = region.Keys
		r.SetRegion(regionInfo)
	}

	return r, nil
}

func (r *RaftInfo) stepRegions(c *ClusterInfo) {
	regions := r.GetRegions()
	for _, region := range regions {
		r.stepLeader(region)
		r.stepSplit(region, c)
	}
}

func (r *RaftInfo) stepLeader(region *core.RegionInfo) {
	if region.Leader != nil && r.conn.nodeHealth(region.Leader.GetStoreId()) {
		return
	}
	newLeader := r.electNewLeader(region)
	region.Leader = newLeader
	if newLeader == nil {
		r.SetRegion(region)
		simutil.Logger.Infof("[region %d] no leader", region.GetId())
		return
	}
	simutil.Logger.Infof("[region %d] elect new leader: %+v,old leader: %+v", region.GetId(), newLeader, region.Leader)
	r.SetRegion(region)
	r.recordRegionChange(region)
}

func (r *RaftInfo) stepSplit(region *core.RegionInfo, c *ClusterInfo) {
	if region.Leader == nil {
		return
	}
	if !c.conf.NeedSplit(region.ApproximateSize, region.ApproximateKeys) {
		return
	}
	ids := make([]uint64, 1+len(region.Peers))
	for i := range ids {
		var err error
		ids[i], err = c.allocID(region.Leader.GetStoreId())
		if err != nil {
			simutil.Logger.Infof("alloc id failed: %s", err)
			return
		}
	}

	region.RegionEpoch.Version++
	region.ApproximateSize /= 2
	region.ApproximateKeys /= 2

	newRegion := region.Clone()
	newRegion.PendingPeers, newRegion.DownPeers = nil, nil
	for i, peer := range newRegion.Peers {
		peer.Id = ids[i]
	}
	newRegion.Id = ids[len(ids)-1]

	splitKey := generateSplitKey(region.StartKey, region.EndKey)
	newRegion.EndKey, region.StartKey = splitKey, splitKey

	r.SetRegion(region)
	r.SetRegion(newRegion)
	r.recordRegionChange(region)
	r.recordRegionChange(newRegion)
}

func (r *RaftInfo) recordRegionChange(region *core.RegionInfo) {
	n := region.Leader.GetStoreId()
	r.regionchange[n] = append(r.regionchange[n], region.Id)
}

func (r *RaftInfo) updateRegionSize(writtenBytes map[string]int64) {
	for key, size := range writtenBytes {
		region := r.SearchRegion([]byte(key))
		if region == nil {
			simutil.Logger.Errorf("region not found for key %s", key)
			continue
		}
		region.ApproximateSize += size
		r.SetRegion(region)
	}
}

func (r *RaftInfo) updateRegionWriteBytes(writeBytes map[uint64]int64) {
	for id, bytes := range writeBytes {
		region := r.GetRegion(id)
		if region == nil {
			simutil.Logger.Errorf("region %d not found", id)
			continue
		}
		region.WrittenBytes = uint64(bytes)
		r.SetRegion(region)
	}
}

func (r *RaftInfo) updateRegionReadBytes(readBytes map[uint64]int64) {
	for id, bytes := range readBytes {
		region := r.GetRegion(id)
		if region == nil {
			simutil.Logger.Errorf("region %d not found", id)
			continue
		}
		region.ReadBytes = uint64(bytes)
		r.SetRegion(region)
	}
}

func (r *RaftInfo) electNewLeader(region *core.RegionInfo) *metapb.Peer {
	var (
		unhealth         int
		newLeaderStoreID uint64
	)
	ids := region.GetStoreIds()
	for id := range ids {
		if r.conn.nodeHealth(id) {
			newLeaderStoreID = id
		} else {
			unhealth++
		}
	}
	if unhealth > len(ids)/2 {
		return nil
	}
	for _, peer := range region.Peers {
		if peer.GetStoreId() == newLeaderStoreID {
			return peer
		}
	}
	return nil
}

const (
	// 26^10 ~= 1.4e+14, should be enough.
	keyChars = "abcdefghijklmnopqrstuvwxyz"
	keyLen   = 10
)

// generate ordered, unique strings.
func generateKeys(size int) []string {
	m := make(map[string]struct{}, size)
	for len(m) < size {
		k := make([]byte, keyLen)
		for i := range k {
			k[i] = keyChars[rand.Intn(len(keyChars))]
		}
		m[string(k)] = struct{}{}
	}

	v := make([]string, 0, size)
	for k := range m {
		v = append(v, k)
	}
	sort.Sort(sort.StringSlice(v))
	return v
}

func generateSplitKey(start, end []byte) []byte {
	var key []byte
	// lessThanEnd is set as true when the key is already less than end key.
	lessThanEnd := len(end) == 0
	for i, s := range start {
		e := byte('z')
		if !lessThanEnd {
			e = end[i]
		}
		c := (s + e) / 2
		key = append(key, c)
		// case1: s = c < e. Continue with lessThanEnd=true.
		// case2: s < c < e. return key.
		// case3: s = c = e. Continue with lessThanEnd=false.
		lessThanEnd = c < e
		if c > s && c < e {
			return key
		}
	}
	key = append(key, ('a'+'z')/2)
	return key
}
