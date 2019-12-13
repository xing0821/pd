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

package api

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/config"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testConfigSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testConfigSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testConfigSuite) TestConfigAll(c *C) {
	addr := fmt.Sprintf("%s/config", s.urlPrefix)
	cfg := &config.Config{}
	err := readJSON(addr, cfg)
	c.Assert(err, IsNil)

	r := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(r)
	c.Assert(err, IsNil)
	err = postJSON(addr, postData)
	c.Assert(err, IsNil)
	time.Sleep(3 * time.Second)
	l := map[string]interface{}{
		"location-labels":       "zone,rack",
		"region-schedule-limit": 10,
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = postJSON(addr, postData)
	c.Assert(err, IsNil)
	time.Sleep(3 * time.Second)
	l = map[string]interface{}{
		"metric-storage": "http://127.0.0.1:9090",
	}
	postData, err = json.Marshal(l)
	c.Assert(err, IsNil)
	err = postJSON(addr, postData)
	c.Assert(err, IsNil)

	time.Sleep(3 * time.Second)
	newCfg := &config.Config{}
	err = readJSON(addr, newCfg)
	c.Assert(err, IsNil)
	cfg.Replication.MaxReplicas = 5
	cfg.Replication.LocationLabels = []string{"zone", "rack"}
	cfg.Schedule.RegionScheduleLimit = 10
	cfg.PDServerCfg.MetricStorage = "http://127.0.0.1:9090"
	c.Assert(cfg, DeepEquals, newCfg)
}

func (s *testConfigSuite) TestConfigSchedule(c *C) {
	addr := fmt.Sprintf("%s/config/schedule", s.urlPrefix)
	sc := &config.ScheduleConfig{}
	c.Assert(readJSON(addr, sc), IsNil)

	sc.MaxStoreDownTime.Duration = time.Second
	postData, err := json.Marshal(sc)
	c.Assert(err, IsNil)
	err = postJSON(addr, postData)
	c.Assert(err, IsNil)

	sc1 := &config.ScheduleConfig{}
	c.Assert(readJSON(addr, sc1), IsNil)
	c.Assert(*sc, DeepEquals, *sc1)
}

func (s *testConfigSuite) TestConfigReplication(c *C) {
	addr := fmt.Sprintf("%s/config/replicate", s.urlPrefix)
	rc := &config.ReplicationConfig{}
	err := readJSON(addr, rc)
	c.Assert(err, IsNil)

	rc.MaxReplicas = 5

	rc1 := map[string]int{"max-replicas": 5}
	postData, err := json.Marshal(rc1)
	c.Assert(err, IsNil)
	err = postJSON(addr, postData)
	c.Assert(err, IsNil)
	rc.LocationLabels = []string{"zone", "rack"}

	rc2 := map[string]interface{}{"location-labels": "zone,rack"}
	postData, err = json.Marshal(rc2)
	c.Assert(err, IsNil)
	err = postJSON(addr, postData)
	c.Assert(err, IsNil)

	rc3 := &config.ReplicationConfig{}
	err = readJSON(addr, rc3)
	c.Assert(err, IsNil)

	c.Assert(*rc, DeepEquals, *rc3)
}

func (s *testConfigSuite) TestConfigLabelProperty(c *C) {
	addr := s.svr.GetAddr() + apiPrefix + "/api/v1/config/label-property"

	loadProperties := func() config.LabelPropertyConfig {
		var cfg config.LabelPropertyConfig
		err := readJSON(addr, &cfg)
		c.Assert(err, IsNil)
		return cfg
	}

	cfg := loadProperties()
	c.Assert(cfg, HasLen, 0)

	cmds := []string{
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "foo", "action": "set", "label-key": "zone", "label-value": "cn2"}`,
		`{"type": "bar", "action": "set", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := postJSON(addr, []byte(cmd))
		c.Assert(err, IsNil)
	}
	cfg = loadProperties()
	c.Assert(cfg, HasLen, 2)
	c.Assert(cfg["foo"], DeepEquals, []config.StoreLabel{
		{Key: "zone", Value: "cn1"},
		{Key: "zone", Value: "cn2"},
	})
	c.Assert(cfg["bar"], DeepEquals, []config.StoreLabel{{Key: "host", Value: "h1"}})

	cmds = []string{
		`{"type": "foo", "action": "delete", "label-key": "zone", "label-value": "cn1"}`,
		`{"type": "bar", "action": "delete", "label-key": "host", "label-value": "h1"}`,
	}
	for _, cmd := range cmds {
		err := postJSON(addr, []byte(cmd))
		c.Assert(err, IsNil)
	}
	cfg = loadProperties()
	c.Assert(cfg, HasLen, 1)
	c.Assert(cfg["foo"], DeepEquals, []config.StoreLabel{{Key: "zone", Value: "cn2"}})
}
