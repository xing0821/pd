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

package health

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api/helper"
	"github.com/pingcap/pd/server/api/util"
)

var _ = Suite(&testHealthAPISuite{})

type testHealthAPISuite struct {
	hc *http.Client
}

func (s *testHealthAPISuite) SetUpSuite(c *C) {
	s.hc = helper.NewHTTPClient()
}

func checkSliceResponse(c *C, body []byte, cfgs []*server.Config, unhealth string) {
	got := []Health{}
	c.Assert(json.Unmarshal(body, &got), IsNil)

	c.Assert(len(got), Equals, len(cfgs))

	for _, h := range got {
		for _, cfg := range cfgs {
			if h.Name != cfg.Name {
				continue
			}
			sortedStringA, sortedStringB := util.RelaxEqualStings(h.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			c.Assert(sortedStringA, Equals, sortedStringB)
		}
		if h.Name == unhealth {
			c.Assert(h.Health, IsFalse)
			continue
		}
		c.Assert(h.Health, IsTrue)
	}
}

func (s *testHealthAPISuite) TestHealthSlice(c *C) {
	cfgs, svrs, clean := helper.MustNewCluster(c, 3)
	defer clean()
	var leader, follow *server.Server

	for _, svr := range svrs {
		if svr.IsLeader() {
			leader = svr
		} else {
			follow = svr
		}
	}
	addr := leader.GetConfig().ClientUrls + "/pd/health"
	follow.Close()
	resp, err := s.hc.Get(addr)
	c.Assert(err, IsNil)
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	checkSliceResponse(c, buf, cfgs, follow.GetConfig().Name)
}
