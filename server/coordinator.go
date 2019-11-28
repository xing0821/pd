// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/server/config"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedulers"
	"github.com/pingcap/pd/server/statistics"
	"go.uber.org/zap"
)

const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	collectTimeout            = 5 * time.Minute
	maxLoadConfigRetries      = 10

	heartbeatChanCapacity = 1024
	patrolScanRegionLimit = 128 // It takes about 14 minutes to iterate 1 million regions.
)

// coordinator is used to manage all schedulers and checkers to decide if the region needs to be scheduled.
type coordinator struct {
	sync.RWMutex

	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	cluster         *RaftCluster
	checkers        *schedule.CheckerController
	regionScatterer *schedule.RegionScatterer
	schedulers      *schedulers.SchedulerController
	opController    *schedule.OperatorController
	hbStreams       *heartbeatStreams
}

// newCoordinator creates a new coordinator.
func newCoordinator(ctx context.Context, cluster *RaftCluster, hbStreams *heartbeatStreams) *coordinator {
	ctx, cancel := context.WithCancel(ctx)
	opController := schedule.NewOperatorController(ctx, cluster, hbStreams)
	return &coordinator{
		ctx:             ctx,
		cancel:          cancel,
		cluster:         cluster,
		checkers:        schedule.NewCheckerController(ctx, cluster, opController),
		regionScatterer: schedule.NewRegionScatterer(cluster),
		schedulers:      schedulers.NewSchedulerController(ctx, cluster, opController, cluster.opt, cluster.storage),
		opController:    opController,
		hbStreams:       hbStreams,
	}
}

// patrolRegions is used to scan regions.
// The checkers will check these regions to decide if they need to do some operations.
func (c *coordinator) patrolRegions() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	timer := time.NewTimer(c.cluster.GetPatrolRegionInterval())
	defer timer.Stop()

	log.Info("coordinator starts patrol regions")
	start := time.Now()
	var key []byte
	for {
		select {
		case <-timer.C:
			timer.Reset(c.cluster.GetPatrolRegionInterval())
		case <-c.ctx.Done():
			log.Info("patrol regions has been stopped")
			return
		}

		regions := c.cluster.ScanRegions(key, nil, patrolScanRegionLimit)
		if len(regions) == 0 {
			// Resets the scan key.
			key = nil
			continue
		}

		for _, region := range regions {
			// Skips the region if there is already a pending operator.
			if c.opController.GetOperator(region.GetID()) != nil {
				continue
			}

			checkerIsBusy, ops := c.checkers.CheckRegion(region)
			if checkerIsBusy {
				break
			}

			key = region.GetEndKey()
			if ops != nil {
				c.opController.AddWaitingOperator(ops...)
			}
		}
		// Updates the label level isolation statistics.
		c.cluster.updateRegionsLabelLevelStats(regions)
		if len(key) == 0 {
			patrolCheckRegionsHistogram.Observe(time.Since(start).Seconds())
			start = time.Now()
		}
	}
}

// drivePushOperator is used to push the unfinished operator to the excutor.
func (c *coordinator) drivePushOperator() {
	defer logutil.LogPanic()

	defer c.wg.Done()
	log.Info("coordinator begins to actively drive push operator")
	ticker := time.NewTicker(schedule.PushOperatorTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			log.Info("drive push operator has been stopped")
			return
		case <-ticker.C:
			c.opController.PushOperators()
		}
	}
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	log.Info("coordinator starts to collect cluster information")
	for {
		if c.shouldRun() {
			log.Info("coordinator has finished cluster information preparation")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			log.Info("coordinator stops running")
			return
		}
	}
	log.Info("coordinator starts to run schedulers")
	var (
		scheduleNames []string
		configs       []string
		err           error
	)
	for i := 0; i < maxLoadConfigRetries; i++ {
		scheduleNames, configs, err = c.cluster.storage.LoadAllScheduleConfig()
		if err == nil {
			break
		}
		log.Error("cannot load schedulers' config", zap.Int("retry-times", i), zap.Error(err))
	}
	if err != nil {
		log.Fatal("cannot load schedulers' config", zap.Error(err))
	}

	scheduleCfg := c.cluster.opt.Load().Clone()
	// The new way to create scheduler with the independent configuration.
	for i, name := range scheduleNames {
		data := configs[i]
		typ := schedule.FindSchedulerTypeByName(name)
		var cfg config.SchedulerConfig
		for _, c := range scheduleCfg.Schedulers {
			if c.Type == typ {
				cfg = c
				break
			}
		}
		if len(cfg.Type) == 0 {
			log.Error("the scheduler type not found", zap.String("scheduler-name", name))
			continue
		}
		if cfg.Disable {
			log.Info("skip create scheduler with independent configuration", zap.String("scheduler-name", name), zap.String("scheduler-type", cfg.Type))
			continue
		}
		s, err := schedule.CreateScheduler(cfg.Type, c.opController, c.cluster.storage, schedule.ConfigJSONDecoder([]byte(data)))
		if err != nil {
			log.Error("can not create scheduler with independent configuration", zap.String("scheduler-name", name), zap.Error(err))
			continue
		}
		log.Info("create scheduler with independent configuration", zap.String("scheduler-name", s.GetName()))
		if err = c.addScheduler(s); err != nil {
			log.Error("can not add scheduler with independent configuration", zap.String("scheduler-name", s.GetName()), zap.Error(err))
		}
	}

	// The old way to create the scheduler.
	k := 0
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		if schedulerCfg.Disable {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
			log.Info("skip create scheduler", zap.String("scheduler-type", schedulerCfg.Type))
			continue
		}

		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.opController, c.cluster.storage, schedule.ConfigSliceDecoder(schedulerCfg.Type, schedulerCfg.Args))
		if err != nil {
			log.Error("can not create scheduler", zap.String("scheduler-type", schedulerCfg.Type), zap.Error(err))
			continue
		}

		log.Info("create scheduler", zap.String("scheduler-name", s.GetName()))
		if err = c.addScheduler(s, schedulerCfg.Args...); err != nil && err != schedulers.ErrSchedulerExisted {
			log.Error("can not add scheduler", zap.String("scheduler-name", s.GetName()), zap.Error(err))
		} else {
			// Only records the valid scheduler config.
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// Removes the invalid scheduler config and persist.
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	c.cluster.opt.Store(scheduleCfg)
	if err := c.cluster.opt.Persist(c.cluster.storage); err != nil {
		log.Error("cannot persist schedule config", zap.Error(err))
	}

	c.wg.Add(2)
	// Starts to patrol regions.
	go c.patrolRegions()
	go c.drivePushOperator()
}

func (c *coordinator) stop() {
	c.cancel()
	c.schedulers.Stop()
}

// Hack to retrieve info from scheduler.
// TODO: remove it.
type hasHotStatus interface {
	GetHotReadStatus() *statistics.StoreHotPeersInfos
	GetHotWriteStatus() *statistics.StoreHotPeersInfos
	GetStoresScore() map[uint64]float64
}

func (c *coordinator) getHotWriteRegions() *statistics.StoreHotPeersInfos {
	c.RLock()
	defer c.RUnlock()

	s := c.schedulers.GetScheduler(schedulers.HotRegionName)
	if s == nil {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotWriteStatus()
	}
	return nil
}

func (c *coordinator) getHotReadRegions() *statistics.StoreHotPeersInfos {
	c.RLock()
	defer c.RUnlock()
	s := c.schedulers.GetScheduler(schedulers.HotRegionName)
	if s == nil {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotReadStatus()
	}
	return nil
}

func (c *coordinator) collectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	c.schedulers.CollectMetrics()
}

func (c *coordinator) resetSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	c.schedulers.ResetMetrics()
}

func (c *coordinator) collectHotSpotMetrics() {
	c.RLock()
	defer c.RUnlock()
	// Collects hot write region metrics.
	s := c.schedulers.GetScheduler(schedulers.HotRegionName)
	if s == nil {
		return
	}
	stores := c.cluster.GetStores()
	status := s.Scheduler.(hasHotStatus).GetHotWriteStatus()
	for _, s := range stores {
		storeAddress := s.GetAddress()
		storeID := s.GetID()
		storeLabel := fmt.Sprintf("%d", storeID)
		stat, ok := status.AsPeer[storeID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_written_bytes_as_peer").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_write_region_as_peer").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_written_bytes_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_write_region_as_peer").Set(0)
		}

		stat, ok = status.AsLeader[storeID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_written_bytes_as_leader").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_write_region_as_leader").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_written_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_write_region_as_leader").Set(0)
		}
	}

	// Collects hot read region metrics.
	status = s.Scheduler.(hasHotStatus).GetHotReadStatus()
	for _, s := range stores {
		storeAddress := s.GetAddress()
		storeID := s.GetID()
		storeLabel := fmt.Sprintf("%d", storeID)
		stat, ok := status.AsLeader[storeID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_read_bytes_as_leader").Set(stat.TotalBytesRate)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_read_region_as_leader").Set(float64(stat.Count))
		} else {
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "total_read_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(storeAddress, storeLabel, "hot_read_region_as_leader").Set(0)
		}
	}

	// Collects score of stores stats metrics.
	scores := s.Scheduler.(hasHotStatus).GetStoresScore()
	for _, store := range stores {
		storeAddress := store.GetAddress()
		storeID := store.GetID()
		score, ok := scores[storeID]
		if ok {
			hotSpotStatusGauge.WithLabelValues(storeAddress, strconv.FormatUint(storeID, 10), "store_score").Set(score)
		}
	}
}

func (c *coordinator) resetHotSpotMetrics() {
	hotSpotStatusGauge.Reset()
}

func (c *coordinator) shouldRun() bool {
	return c.cluster.isPrepared()
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()
	return c.schedulers.AddScheduler(scheduler, args...)
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return ErrNotBootstrapped
	}
	return c.schedulers.RemoveScheduler(name)
}

func (c *coordinator) pauseOrResumeScheduler(name string, t int64) error {
	c.Lock()
	defer c.Unlock()
	if c.cluster == nil {
		return ErrNotBootstrapped
	}
	return c.schedulers.PauseOrResumeScheduler(name, t)
}

func (c *coordinator) getSchedulers() []string {
	c.RLock()
	defer c.RUnlock()
	return c.schedulers.GetSchedulers()
}

func (c *coordinator) getScheduler(name string) *schedulers.Scheduler {
	c.RLock()
	defer c.RUnlock()
	return c.schedulers.GetScheduler(name)
}
