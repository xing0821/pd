// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/statistics"
)

// Cluster provides an overview of a cluster's regions distribution.
type Cluster interface {
	core.RegionSetInformer
	core.StoreSetInformer
	core.StoreSetController

	statistics.RegionStatInformer
	opt.Options

	// get config methods
	GetOpt() namespace.ScheduleOptions
	// TODO: it should be removed. Schedulers don't need to know anything
	// about peers.
	AllocPeer(storeID uint64) (*metapb.Peer, error)
}
