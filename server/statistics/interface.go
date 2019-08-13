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

package statistics

import "github.com/pingcap/pd/server/core"

// Classifier is used to determine the namespace which the store or region
// belongs.
type Classifier interface {
	GetAllNamespaces() []string
	GetStoreNamespace(*core.StoreInfo) string
	GetRegionNamespace(*core.RegionInfo) string
	IsNamespaceExist(name string) bool
	AllowMerge(*core.RegionInfo, *core.RegionInfo) bool
	// Reload underlying namespaces
	ReloadNamespaces() error
	// These function below are only for tests
	IsMetaExist() bool
	IsTableIDExist(int64) bool
	IsStoreIDExist(uint64) bool
}

// RegionStatInformer provides access to a shared informer of statistics.
type RegionStatInformer interface {
	IsRegionHot(region *core.RegionInfo) bool
	RegionWriteStats() map[uint64][]*HotSpotPeerStat
	RegionReadStats() map[uint64][]*HotSpotPeerStat
	RandHotRegionFromStore(store uint64, kind FlowKind) *core.RegionInfo
}
