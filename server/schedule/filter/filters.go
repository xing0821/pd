// Copyright 2016 PingCAP, Inc.
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

package filter

import (
	"fmt"

	"github.com/pingcap/pd/pkg/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule/opt"
)

//revive:disable:unused-parameter

// Filter is an interface to filter source and target store.
type Filter interface {
	// ActOn is used to indicate where the filter will act on.
	ActOn() string
	Type() string
	// Return true if the store should not be used as a source store.
	Source(opt opt.Options, store *core.StoreInfo) bool
	// Return true if the store should not be used as a target store.
	Target(opt opt.Options, store *core.StoreInfo) bool
}

// Source checks if store can pass all Filters as source store.
func Source(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.Source(opt, store) {
			filterCounter.WithLabelValues("filter-source", storeAddress, storeID, filter.ActOn(), filter.Type()).Inc()
			return true
		}
	}
	return false
}

// Target checks if store can pass all Filters as target store.
func Target(opt opt.Options, store *core.StoreInfo, filters []Filter) bool {
	storeAddress := store.GetAddress()
	storeID := fmt.Sprintf("%d", store.GetID())
	for _, filter := range filters {
		if filter.Target(opt, store) {
			filterCounter.WithLabelValues("filter-target", storeAddress, storeID, filter.ActOn(), filter.Type()).Inc()
			return true
		}
	}
	return false
}

type excludedFilter struct {
	actOn   string
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

// NewExcludedFilter creates a Filter that filters all specified stores.
func NewExcludedFilter(actOn string, sources, targets map[uint64]struct{}) Filter {
	return &excludedFilter{
		actOn:   actOn,
		sources: sources,
		targets: targets,
	}
}

func (f *excludedFilter) ActOn() string {
	return f.actOn
}

func (f *excludedFilter) Type() string {
	return "exclude-filter"
}

func (f *excludedFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.sources[store.GetID()]
	return ok
}

func (f *excludedFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	_, ok := f.targets[store.GetID()]
	return ok
}

type overloadFilter struct{ actOn string }

// NewOverloadFilter creates a Filter that filters all stores that are overloaded from balance.
func NewOverloadFilter(actOn string) Filter {
	return &overloadFilter{actOn: actOn}
}

func (f *overloadFilter) ActOn() string {
	return f.actOn
}

func (f *overloadFilter) Type() string {
	return "overload-filter"
}

func (f *overloadFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

func (f *overloadFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsOverloaded()
}

type stateFilter struct{ actOn string }

// NewStateFilter creates a Filter that filters all stores that are not UP.
func NewStateFilter(actOn string) Filter {
	return &stateFilter{actOn: actOn}
}

func (f *stateFilter) ActOn() string {
	return f.actOn
}

func (f *stateFilter) Type() string {
	return "state-filter"
}

func (f *stateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsTombstone()
}

func (f *stateFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return !store.IsUp()
}

type healthFilter struct{ actOn string }

// NewHealthFilter creates a Filter that filters all stores that are Busy or Down.
func NewHealthFilter(actOn string) Filter {
	return &healthFilter{actOn: actOn}
}

func (f *healthFilter) ActOn() string {
	return f.actOn
}

func (f *healthFilter) Type() string {
	return "health-filter"
}

func (f *healthFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}
	return store.DownTime() > opt.GetMaxStoreDownTime()
}

func (f *healthFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *healthFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type pendingPeerCountFilter struct{ actOn string }

// NewPendingPeerCountFilter creates a Filter that filters all stores that are
// currently handling too many pending peers.
func NewPendingPeerCountFilter(actOn string) Filter {
	return &pendingPeerCountFilter{actOn: actOn}
}

func (p *pendingPeerCountFilter) ActOn() string {
	return p.actOn
}

func (p *pendingPeerCountFilter) Type() string {
	return "pending-peer-filter"
}

func (p *pendingPeerCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	if opt.GetMaxPendingPeerCount() == 0 {
		return false
	}
	return store.GetPendingPeerCount() > int(opt.GetMaxPendingPeerCount())
}

func (p *pendingPeerCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

func (p *pendingPeerCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return p.filter(opt, store)
}

type snapshotCountFilter struct{ actOn string }

// NewSnapshotCountFilter creates a Filter that filters all stores that are
// currently handling too many snapshots.
func NewSnapshotCountFilter(actOn string) Filter {
	return &snapshotCountFilter{actOn: actOn}
}

func (f *snapshotCountFilter) ActOn() string {
	return f.actOn
}

func (f *snapshotCountFilter) Type() string {
	return "snapshot-filter"
}

func (f *snapshotCountFilter) filter(opt opt.Options, store *core.StoreInfo) bool {
	return uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount()
}

func (f *snapshotCountFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

func (f *snapshotCountFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(opt, store)
}

type cacheFilter struct {
	actOn string
	cache *cache.TTLUint64
}

// NewCacheFilter creates a Filter that filters all stores that are in the cache.
func NewCacheFilter(actOn string, cache *cache.TTLUint64) Filter {
	return &cacheFilter{actOn: actOn, cache: cache}
}

func (f *cacheFilter) ActOn() string {
	return f.actOn
}

func (f *cacheFilter) Type() string {
	return "cache-filter"
}

func (f *cacheFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.cache.Exists(store.GetID())
}

func (f *cacheFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

type storageThresholdFilter struct{ actOn string }

// NewStorageThresholdFilter creates a Filter that filters all stores that are
// almost full.
func NewStorageThresholdFilter(actOn string) Filter {
	return &storageThresholdFilter{actOn: actOn}
}

func (f *storageThresholdFilter) ActOn() string {
	return f.actOn
}

func (f *storageThresholdFilter) Type() string {
	return "storage-threshold-filter"
}

func (f *storageThresholdFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return store.IsLowSpace(opt.GetLowSpaceRatio())
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	actOn     string
	labels    []string
	stores    []*core.StoreInfo
	safeScore float64
}

// NewDistinctScoreFilter creates a filter that filters all stores that have
// lower distinct score than specified store.
func NewDistinctScoreFilter(actOn string, labels []string, stores []*core.StoreInfo, source *core.StoreInfo) Filter {
	newStores := make([]*core.StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.GetID() == source.GetID() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		actOn:     actOn,
		labels:    labels,
		stores:    newStores,
		safeScore: core.DistinctScore(labels, newStores, source),
	}
}

func (f *distinctScoreFilter) ActOn() string {
	return f.actOn
}

func (f *distinctScoreFilter) Type() string {
	return "distinct-filter"
}

func (f *distinctScoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return core.DistinctScore(f.labels, f.stores, store) < f.safeScore
}

type namespaceFilter struct {
	actOn      string
	classifier namespace.Classifier
	namespace  string
}

// NewNamespaceFilter creates a Filter that filters all stores that are not
// belong to a namespace.
func NewNamespaceFilter(actOn string, classifier namespace.Classifier, namespace string) Filter {
	return &namespaceFilter{
		actOn:      actOn,
		classifier: classifier,
		namespace:  namespace,
	}
}

func (f *namespaceFilter) ActOn() string {
	return f.actOn
}

func (f *namespaceFilter) Type() string {
	return "namespace-filter"
}

func (f *namespaceFilter) filter(store *core.StoreInfo) bool {
	return f.classifier.GetStoreNamespace(store) != f.namespace
}

func (f *namespaceFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

func (f *namespaceFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	return f.filter(store)
}

// StoreStateFilter is used to determine whether a store can be selected as the
// source or target of the schedule based on the store's state.
type StoreStateFilter struct {
	Act string
	// Set true if the schedule involves any transfer leader operation.
	TransferLeader bool
	// Set true if the schedule involves any move region operation.
	MoveRegion bool
}

// ActOn returns the scheduler or the checker which the filter acts on.
func (f StoreStateFilter) ActOn() string {
	return f.Act
}

// Type returns the type of the Filter.
func (f StoreStateFilter) Type() string {
	return "store-state-filter"
}

// Source returns true when the store cannot be selected as the schedule
// source.
func (f StoreStateFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.DownTime() > opt.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader && (store.IsDisconnected() || store.IsBlocked()) {
		return true
	}

	if f.MoveRegion && f.filterMoveRegion(opt, store) {
		return true
	}
	return false
}

// Target returns true when the store cannot be selected as the schedule
// target.
func (f StoreStateFilter) Target(opts opt.Options, store *core.StoreInfo) bool {
	if store.IsTombstone() ||
		store.IsOffline() ||
		store.DownTime() > opts.GetMaxStoreDownTime() {
		return true
	}
	if f.TransferLeader &&
		(store.IsDisconnected() ||
			store.IsBlocked() ||
			store.GetIsBusy() ||
			opts.CheckLabelProperty(opt.RejectLeader, store.GetLabels())) {
		return true
	}

	if f.MoveRegion {
		// only target consider the pending peers because pending more means the disk is slower.
		if opts.GetMaxPendingPeerCount() > 0 && store.GetPendingPeerCount() > int(opts.GetMaxPendingPeerCount()) {
			return true
		}

		if f.filterMoveRegion(opts, store) {
			return true
		}
	}
	return false
}

func (f StoreStateFilter) filterMoveRegion(opt opt.Options, store *core.StoreInfo) bool {
	if store.GetIsBusy() {
		return true
	}

	if store.IsOverloaded() {
		return true
	}

	if uint64(store.GetSendingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetReceivingSnapCount()) > opt.GetMaxSnapshotCount() ||
		uint64(store.GetApplyingSnapCount()) > opt.GetMaxSnapshotCount() {
		return true
	}
	return false
}

// BlacklistType the type of BlackListStore Filter.
type BlacklistType int

// some flags about blacklist type.
const (
	// blacklist associated with the source.
	BlacklistSource BlacklistType = 1 << iota
	// blacklist associated with the target.
	BlacklistTarget
)

// BlacklistStoreFilter filters the store according to the blacklist.
type BlacklistStoreFilter struct {
	actOn     string
	blacklist map[uint64]struct{}
	flag      BlacklistType
}

// NewBlacklistStoreFilter creates a blacklist filter.
func NewBlacklistStoreFilter(actOn string, typ BlacklistType) *BlacklistStoreFilter {
	return &BlacklistStoreFilter{
		actOn:     actOn,
		blacklist: make(map[uint64]struct{}),
		flag:      typ,
	}
}

// ActOn returns the scheduler or the checker which the filter acts on.
func (f *BlacklistStoreFilter) ActOn() string {
	return f.actOn
}

// Type implements the Filter.
func (f *BlacklistStoreFilter) Type() string {
	return "blacklist-store-filter"
}

// Source implements the Filter.
func (f *BlacklistStoreFilter) Source(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistSource != BlacklistSource {
		return false
	}
	return f.filter(store)
}

// Add adds the store to the blacklist.
func (f *BlacklistStoreFilter) Add(storeID uint64) {
	f.blacklist[storeID] = struct{}{}
}

// Target implements the Filter.
func (f *BlacklistStoreFilter) Target(opt opt.Options, store *core.StoreInfo) bool {
	if f.flag&BlacklistTarget != BlacklistTarget {
		return false
	}
	return f.filter(store)
}

func (f *BlacklistStoreFilter) filter(store *core.StoreInfo) bool {
	_, ok := f.blacklist[store.GetID()]
	return ok
}
