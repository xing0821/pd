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
	"sync/atomic"
	"time"

	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pingcap/pd/server/schedule/opt"
)

const maxScheduleRetries = 10

// SchedulerController is used to manage a scheduler to schedule.
type SchedulerController struct {
	schedule.Scheduler
	cluster      opt.Cluster
	opController *schedule.OperatorController
	nextInterval time.Duration
	delayUntil   int64
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewSchedulerController creates a new scheduleController.
func NewSchedulerController(ctx context.Context, cluster opt.Cluster, opController *schedule.OperatorController, s schedule.Scheduler) *SchedulerController {
	ctx, cancel := context.WithCancel(ctx)
	return &SchedulerController{
		Scheduler:    s,
		cluster:      cluster,
		opController: opController,
		nextInterval: s.GetMinInterval(),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Ctx return the context of scheduler controller.
func (s *SchedulerController) Ctx() context.Context {
	return s.ctx
}

// Stop stops the scheduler controller.
func (s *SchedulerController) Stop() {
	s.cancel()
}

// Schedule returns operators created by each scheduler.
func (s *SchedulerController) Schedule() []*operator.Operator {
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
func (s *SchedulerController) GetInterval() time.Duration {
	return s.nextInterval
}

// AllowSchedule returns if a scheduler is allowed to schedule.
func (s *SchedulerController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster)
}

// SetDelay sets the delay time.
func (s *SchedulerController) SetDelay(delayUntil int64) {
	atomic.StoreInt64(&s.delayUntil, delayUntil)
}