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

package cluster

import (
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api/helper"
	"github.com/pingcap/pd/server/api/util"
)

var _ = Suite(&testClusterInfo{})

type testClusterInfo struct {
	svr       *server.Server
	cleanup   func()
	urlPrefix string
}

func (s *testClusterInfo) SetUpSuite(c *C) {
	s.svr, s.cleanup = helper.MustNewServer(c)
	helper.MustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s/pd/api/v1", addr)
}

func (s *testClusterInfo) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterInfo) TestCluster(c *C) {
	url := fmt.Sprintf("%s/cluster", s.urlPrefix)
	c1 := &metapb.Cluster{}
	err := util.ReadJSONWithURL(url, c1)
	c.Assert(err, IsNil)

	c2 := &metapb.Cluster{}
	r := server.ReplicationConfig{MaxReplicas: 6}
	c.Assert(s.svr.SetReplicationConfig(r), IsNil)
	err = util.ReadJSONWithURL(url, c2)
	c.Assert(err, IsNil)

	c1.MaxPeerCount = 6
	c.Assert(c1, DeepEquals, c2)
}

func (s *testClusterInfo) TestGetClusterStatus(c *C) {
	url := fmt.Sprintf("%s/cluster/status", s.urlPrefix)
	status := server.ClusterStatus{}
	err := util.ReadJSONWithURL(url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.IsZero(), IsTrue)
	now := time.Now()
	helper.MustBootstrapCluster(c, s.svr)
	err = util.ReadJSONWithURL(url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.After(now), IsTrue)
}
