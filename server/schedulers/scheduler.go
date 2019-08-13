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

package schedulers

import (
	"context"
	"math/rand"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	"github.com/pingcap/pd/server/schedule/operator"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const maxScheduleRetries = 10

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	// GetType should in accordance with the name passing to RegisterScheduler()
	GetType() string
	GetMinInterval() time.Duration
	GetNextInterval(interval time.Duration) time.Duration
	Prepare(cluster schedule.Cluster) error
	Cleanup(cluster schedule.Cluster)
	Schedule(cluster schedule.Cluster) []*operator.Operator
	IsScheduleAllowed(cluster schedule.Cluster) bool
}

// CreateSchedulerFunc is for creating scheduler.
type CreateSchedulerFunc func(opController *schedule.OperatorController, args []string) (Scheduler, error)

var schedulerMap = make(map[string]CreateSchedulerFunc)

// RegisterScheduler binds a scheduler creator. It should be called in init()
// func of a package.
func RegisterScheduler(name string, createFn CreateSchedulerFunc) {
	if _, ok := schedulerMap[name]; ok {
		log.Fatal("duplicated scheduler", zap.String("name", name))
	}
	schedulerMap[name] = createFn
}

// IsSchedulerRegistered check where the named scheduler type is registered.
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap[name]
	return ok
}

// CreateScheduler creates a scheduler with registered creator func.
func CreateScheduler(name string, opController *schedule.OperatorController, args ...string) (Scheduler, error) {
	fn, ok := schedulerMap[name]
	if !ok {
		return nil, errors.Errorf("create func of %v is not registered", name)
	}
	return fn(opController, args)
}

// SchedulerController is used to manage a scheduler to schedule.
type SchedulerController struct {
	Scheduler
	cluster      schedule.Cluster
	opController *schedule.OperatorController
	classifier   namespace.Classifier
	nextInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewSchedulerController creates a new SchedulerController.
func NewSchedulerController(ctx context.Context, cluster schedule.Cluster, opController *schedule.OperatorController, classifier namespace.Classifier, s Scheduler) *SchedulerController {
	ctx, cancel := context.WithCancel(ctx)
	return &SchedulerController{
		Scheduler:    s,
		cluster:      cluster,
		opController: opController,
		nextInterval: s.GetMinInterval(),
		classifier:   classifier,
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
		if op := ScheduleByNamespace(s.cluster, s.classifier, s.Scheduler); op != nil {
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

// SetInterval sets the interval of scheduling for a scheduler. Only for test purpose.
func (s *SchedulerController) SetInterval(t time.Duration) {
	s.nextInterval = t
}

// AllowSchedule returns if a scheduler is allowed to schedule.
func (s *SchedulerController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster)
}

// ScheduleByNamespace schedule according to the namespace.
func ScheduleByNamespace(cluster schedule.Cluster, classifier namespace.Classifier, scheduler Scheduler) []*operator.Operator {
	namespaces := classifier.GetAllNamespaces()
	for _, i := range rand.Perm(len(namespaces)) {
		nc := namespace.NewCluster(cluster, classifier, namespaces[i])
		if op := scheduler.Schedule(nc); op != nil {
			return op
		}
	}
	return nil
}
