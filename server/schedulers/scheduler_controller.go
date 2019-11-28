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

package schedulers

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const maxScheduleRetries = 10

var (
	// ErrSchedulerExisted is error info for scheduler is existed.
	ErrSchedulerExisted = errors.New("scheduler existed")
	// ErrSchedulerNotFound is error info for scheduler is not found.
	ErrSchedulerNotFound = errors.New("scheduler not found")
)

// Options for SchedulerController.
type Options interface {
	AddSchedulerCfg(tp string, args []string)
	RemoveSchedulerCfg(ctx context.Context, name string) error

	Persist(storage *core.Storage) error
}

// SchedulerController is used to manage a scheduler to schedule.
type SchedulerController struct {
	sync.RWMutex

	wg           sync.WaitGroup
	schedulers   map[string]*Scheduler
	opt          Options
	cluster      opt.Cluster
	storage      *core.Storage
	opController *schedule.OperatorController
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewSchedulerController creates a new scheduleController.
func NewSchedulerController(ctx context.Context, cluster opt.Cluster, opController *schedule.OperatorController, opt Options, s *core.Storage) *SchedulerController {
	ctx, cancel := context.WithCancel(ctx)
	return &SchedulerController{
		schedulers:   make(map[string]*Scheduler),
		cluster:      cluster,
		opt:          opt,
		storage:      s,
		opController: opController,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// GetSchedulers returns all schedulers' name.
func (sc *SchedulerController) GetSchedulers() []string {
	sc.RLock()
	defer sc.RUnlock()
	names := make([]string, 0, len(sc.schedulers))
	for name := range sc.schedulers {
		names = append(names, name)
	}
	return names
}

// GetScheduler gets a scheduler with a given name.
func (sc *SchedulerController) GetScheduler(name string) *Scheduler {
	sc.RLock()
	defer sc.RUnlock()
	s, ok := sc.schedulers[name]
	if !ok {
		return nil
	}
	return s
}

// AddScheduler adds a scheduler.
func (sc *SchedulerController) AddScheduler(scheduler schedule.Scheduler, args ...string) error {
	if s := sc.GetScheduler(scheduler.GetName()); s != nil {
		return ErrSchedulerExisted
	}
	sc.Lock()
	defer sc.Unlock()
	s := NewScheduler(sc, scheduler)
	if err := s.Prepare(sc.cluster); err != nil {
		return err
	}

	sc.wg.Add(1)
	go sc.runScheduler(s)
	sc.schedulers[s.GetName()] = s
	sc.opt.AddSchedulerCfg(s.GetType(), args)
	return nil
}

// RemoveScheduler removes a scheduler with a given name.
func (sc *SchedulerController) RemoveScheduler(name string) error {
	var s *Scheduler
	if s = sc.GetScheduler(name); s == nil {
		return ErrSchedulerNotFound
	}

	sc.Lock()
	defer sc.Unlock()
	s.Stop()
	schedulerStatusGauge.WithLabelValues(name, "allow").Set(0)
	delete(sc.schedulers, name)

	var err error
	if err = sc.opt.RemoveSchedulerCfg(s.Ctx(), name); err != nil {
		log.Error("can not remove scheduler", zap.String("scheduler-name", name), zap.Error(err))
	} else if err = sc.opt.Persist(sc.storage); err != nil {
		log.Error("the option can not persist scheduler config", zap.Error(err))
	} else {
		err = sc.storage.RemoveScheduleConfig(name)
		if err != nil {
			log.Error("can not remove the scheduler config", zap.Error(err))
		}
	}
	return err
}

// PauseOrResumeScheduler pauses or resumes a scheduler.
func (sc *SchedulerController) PauseOrResumeScheduler(name string, t int64) error {
	s := make([]*Scheduler, 0)
	if name != "all" {
		sc := sc.GetScheduler(name)
		if sc == nil {
			return ErrSchedulerNotFound
		}
		s = append(s, sc)
	} else {
		for _, scheduler := range sc.schedulers {
			s = append(s, scheduler)
		}
	}
	var err error
	for _, sc := range s {
		var delayUntil int64 = 0
		if t > 0 {
			delayUntil = time.Now().Unix() + t
		}
		sc.SetDelay(delayUntil)
	}
	return err
}

func (sc *SchedulerController) runScheduler(s *Scheduler) {
	defer logutil.LogPanic()
	defer sc.wg.Done()
	defer s.Cleanup(sc.cluster)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			if op := s.Schedule(); op != nil {
				sc.opController.AddWaitingOperator(op...)
			}

		case <-s.Ctx().Done():
			log.Info("scheduler has been stopped",
				zap.String("scheduler-name", s.GetName()),
				zap.Error(s.Ctx().Err()))
			return
		}
	}
}

// CollectMetrics collects metrics.
func (sc *SchedulerController) CollectMetrics() {
	sc.RLock()
	defer sc.RUnlock()
	for _, s := range sc.schedulers {
		var allowScheduler float64
		// If the scheduler is not allowed to schedule, it will disappear in Grafana panel.
		// See issue #1341.
		if s.AllowSchedule() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.GetName(), "allow").Set(allowScheduler)
	}
}

// ResetMetrics resets metrics.
func (sc *SchedulerController) ResetMetrics() {
	schedulerStatusGauge.Reset()
}

// Stop will stop SchedulerController and wait all schedulers stop.
func (sc *SchedulerController) Stop() {
	sc.cancel()
	sc.wg.Wait()
}

// Scheduler is a warpper to manage the scheduler.
type Scheduler struct {
	schedule.Scheduler
	cluster      opt.Cluster
	opController *schedule.OperatorController
	nextInterval time.Duration
	delayUntil   int64
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewScheduler creates a new scheduler.
func NewScheduler(sc *SchedulerController, s schedule.Scheduler) *Scheduler {
	ctx, cancel := context.WithCancel(sc.ctx)
	return &Scheduler{
		Scheduler:    s,
		cluster:      sc.cluster,
		opController: sc.opController,
		nextInterval: s.GetMinInterval(),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Ctx return the context of scheduler controller.
func (s *Scheduler) Ctx() context.Context {
	return s.ctx
}

// Stop stops the scheduler controller.
func (s *Scheduler) Stop() {
	s.cancel()
}

// Schedule returns operators created by each scheduler.
func (s *Scheduler) Schedule() []*operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		if op := s.Scheduler.Schedule(s.cluster); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

// GetInterval returns the interval of scheduling for a scheduler.
func (s *Scheduler) GetInterval() time.Duration {
	return s.nextInterval
}

// AllowSchedule returns if a scheduler is allowed to schedule.
func (s *Scheduler) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster)
}

// SetDelay sets the delay time.
func (s *Scheduler) SetDelay(delayUntil int64) {
	atomic.StoreInt64(&s.delayUntil, delayUntil)
}

// IsPaused return if the scheduler is paused.
func (s *Scheduler) IsPaused() bool {
	delayUntil := atomic.LoadInt64(&s.delayUntil)
	return time.Now().Unix() < delayUntil
}
