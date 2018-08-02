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

package server

import (
	"path"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	log "github.com/sirupsen/logrus"
)

const (
	// update timestamp every updateTimestampStep.
	updateTimestampStep  = 50 * time.Millisecond
	updateTimestampGuard = time.Millisecond
	maxLogical           = int64(1 << 18)
)

var (
	zeroTime = time.Time{}
)

type atomicObject struct {
	physical time.Time
	logical  int64
}

func (s *Server) getTimestampPath() string {
	return path.Join(s.rootPath, "timestamp")
}

func (s *Server) loadTimestamp() (time.Time, error) {
	data, err := getValue(s.client, s.getTimestampPath())
	if err != nil {
		return zeroTime, errors.Trace(err)
	}
	if len(data) == 0 {
		return zeroTime, nil
	}
	return parseTimestamp(data)
}

// save timestamp, if lastTs is 0, we think the timestamp doesn't exist, so create it,
// otherwise, update it.
func (s *Server) saveTimestamp(ts time.Time) error {
	data := uint64ToBytes(uint64(ts.UnixNano()))
	key := s.getTimestampPath()

	resp, err := s.leaderTxn().Then(clientv3.OpPut(key, string(data))).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("save timestamp failed, maybe we lost leader")
	}

	s.lastSavedTime.Store(ts)

	return nil
}

func (s *Server) syncTimestamp() error {
	tsoCounter.WithLabelValues("sync").Inc()

	last, err := s.loadTimestamp()
	if err != nil {
		return errors.Trace(err)
	}

	now := time.Now()
	// If the current system time minus the saved etcd timestamp is less than updateTimestampGuard,
	// the timestamp allocation will start from the saved etcd timestamp temporarily.
	if subTimeByWallClock(now, last) < updateTimestampGuard {
		log.Errorf("system time may be incorrect, last: %v, now: %v", last, now)
		last = last.Add(updateTimestampGuard)
		save := last.Add(s.cfg.TsoSaveInterval.Duration)
		if err = s.saveTimestamp(save); err != nil {
			return errors.Trace(err)
		}
		s.ts.Store(&atomicObject{physical: last})
		return nil
	}

	save := now.Add(s.cfg.TsoSaveInterval.Duration)
	if err = s.saveTimestamp(save); err != nil {
		return errors.Trace(err)
	}

	tsoCounter.WithLabelValues("sync_ok").Inc()
	log.Infof("sync and save timestamp: last %v save %v", last, save)

	current := &atomicObject{
		physical: now,
	}
	s.ts.Store(current)

	return nil
}

func (s *Server) updateTimestamp() error {
	prev := s.ts.Load().(*atomicObject).physical
	now := time.Now()

	tsoCounter.WithLabelValues("save").Inc()

	since := subTimeByWallClock(now, prev)
	if since > 3*updateTimestampStep {
		log.Warnf("clock offset: %v, prev: %v, now: %v", since, prev, now)
		tsoCounter.WithLabelValues("slow_save").Inc()
	}

	current := &atomicObject{
		physical: now,
	}

	saved := s.lastSavedTime.Load().(time.Time)
	// This is a normal situation.
	if since > 0 {
		// Avoid the same physical timestamp.
		if since <= updateTimestampGuard {
			tsoCounter.WithLabelValues("skip_save").Inc()
			log.Warnf("invalid physical timestamp, prev: %v, now: %v, re-update later", prev, now)
			return nil
		}
		// If the time window is no more than updateTimestampGuard,
		// it will adjust the time window.
		if subTimeByWallClock(saved, now) <= updateTimestampGuard {
			last := saved
			save := now.Add(s.cfg.TsoSaveInterval.Duration)
			if err := s.saveTimestamp(save); err != nil {
				return errors.Trace(err)
			}

			tsoCounter.WithLabelValues("save_ok").Inc()
			log.Debugf("save timestamp ok: prev %v last %v save %v", prev, last, save)
		}
	} else {
		if subTimeByWallClock(saved, prev) <= updateTimestampGuard {
			last := saved
			save := prev.Add(s.cfg.TsoSaveInterval.Duration)
			if err := s.saveTimestamp(save); err != nil {
				return errors.Trace(err)
			}

			tsoCounter.WithLabelValues("save_incorrect").Inc()
			log.Debugf("save timestamp incorrect: now %v prev %v last %v save %v", now, prev, last, save)
		}
		// Since the physical timestamp is greater than the current system time,
		// the physical timestamp will be used to alloc timestamp temporarily.
		current.physical = prev
	}

	s.ts.Store(current)
	metadataGauge.WithLabelValues("tso").Set(float64(current.physical.Unix()))

	return nil
}

const maxRetryCount = 100

func (s *Server) getRespTS(count uint32) (pdpb.Timestamp, error) {
	var resp pdpb.Timestamp
	for i := 0; i < maxRetryCount; i++ {
		current, ok := s.ts.Load().(*atomicObject)
		if !ok || current.physical == zeroTime {
			log.Errorf("we haven't synced timestamp ok, wait and retry, retry count %d", i)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		resp.Physical = current.physical.UnixNano() / int64(time.Millisecond)
		resp.Logical = atomic.AddInt64(&current.logical, int64(count))

		if resp.Logical >= maxLogical {
			// If the time window is enough, it will increase the physical time,
			// otherwise, it will wait for updating the time window.
			saved := s.lastSavedTime.Load().(time.Time)
			if subTimeByWallClock(saved, current.physical) > updateTimestampGuard {
				last := &atomicObject{
					physical: current.physical.Add(time.Millisecond),
					logical:  0,
				}
				s.ts.Store(last)
			} else {
				log.Errorf("logical part outside of max logical interval %v, please check ntp time, retry count %d", resp, i)
				time.Sleep(updateTimestampStep)
				continue
			}
		}
		return resp, nil
	}
	return resp, errors.New("can not get timestamp")
}
