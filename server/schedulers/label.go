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

package schedulers

import (
	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/filter"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pingcap/pd/server/schedule/selector"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const labelSchedulerName = "label-scheduler"

func init() {
	schedule.RegisterSliceDecoderBuilder("label", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			conf, ok := v.(*labelSchedulerConfig)
			if !ok {
				return ErrScheduleConfigNotExist
			}
			ranges, err := getKeyRanges(args)
			if err != nil {
				return errors.WithStack(err)
			}
			conf.Ranges = ranges
			conf.Name = labelSchedulerName
			return nil
		}
	})

	schedule.RegisterScheduler("label", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		conf := &labelSchedulerConfig{}
		decoder(conf)
		return newLabelScheduler(opController, conf), nil
	})
}

type labelSchedulerConfig struct {
	Name   string          `json:"name"`
	Ranges []core.KeyRange `json:"ranges"`
}

type labelScheduler struct {
	*baseScheduler
	conf     *labelSchedulerConfig
	selector *selector.BalanceSelector
}

// LabelScheduler is mainly based on the store's label information for scheduling.
// Now only used for reject leader schedule, that will move the leader out of
// the store with the specific label.
func newLabelScheduler(opController *schedule.OperatorController, conf *labelSchedulerConfig) schedule.Scheduler {
	filters := []filter.Filter{
		filter.StoreStateFilter{ActionScope: labelSchedulerName, TransferLeader: true},
	}
	kind := core.NewScheduleKind(core.LeaderKind, core.ByCount)
	return &labelScheduler{
		baseScheduler: newBaseScheduler(opController),
		conf:          conf,
		selector:      selector.NewBalanceSelector(kind, filters),
	}
}

func (s *labelScheduler) GetName() string {
	return s.conf.Name
}

func (s *labelScheduler) GetType() string {
	return "label"
}

func (s *labelScheduler) EncodeConfig() ([]byte, error) {
	return schedule.EncodeConfig(s.conf)
}

func (s *labelScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpLeader) < cluster.GetLeaderScheduleLimit()
}

func (s *labelScheduler) Schedule(cluster opt.Cluster) []*operator.Operator {
	schedulerCounter.WithLabelValues(s.GetName(), "schedule").Inc()
	stores := cluster.GetStores()
	rejectLeaderStores := make(map[uint64]struct{})
	for _, s := range stores {
		if cluster.CheckLabelProperty(opt.RejectLeader, s.GetLabels()) {
			rejectLeaderStores[s.GetID()] = struct{}{}
		}
	}
	if len(rejectLeaderStores) == 0 {
		schedulerCounter.WithLabelValues(s.GetName(), "skip").Inc()
		return nil
	}
	log.Debug("label scheduler reject leader store list", zap.Reflect("stores", rejectLeaderStores))
	for id := range rejectLeaderStores {
		if region := cluster.RandLeaderRegion(id, s.conf.Ranges); region != nil {
			log.Debug("label scheduler selects region to transfer leader", zap.Uint64("region-id", region.GetID()))
			excludeStores := make(map[uint64]struct{})
			for _, p := range region.GetDownPeers() {
				excludeStores[p.GetPeer().GetStoreId()] = struct{}{}
			}
			for _, p := range region.GetPendingPeers() {
				excludeStores[p.GetStoreId()] = struct{}{}
			}
			f := filter.NewExcludedFilter(s.GetName(), nil, excludeStores)
			target := s.selector.SelectTarget(cluster, cluster.GetFollowerStores(region), f)
			if target == nil {
				log.Debug("label scheduler no target found for region", zap.Uint64("region-id", region.GetID()))
				schedulerCounter.WithLabelValues(s.GetName(), "no-target").Inc()
				continue
			}

			schedulerCounter.WithLabelValues(s.GetName(), "new-operator").Inc()
			op := operator.CreateTransferLeaderOperator("label-reject-leader", region, id, target.GetID(), operator.OpLeader)
			return []*operator.Operator{op}
		}
	}
	schedulerCounter.WithLabelValues(s.GetName(), "no-region").Inc()
	return nil
}
