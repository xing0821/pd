package server

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testGetStoresSuite{})

type testGetStoresSuite struct {
	cluster *clusterInfo
}

func (s *testGetStoresSuite) SetUpSuite(c *C) {
	_, opt, err := newTestScheduleConfig()
	c.Assert(err, IsNil)
	s.cluster = newClusterInfo(core.NewMockIDAllocator(), opt, core.NewKV(core.NewMemoryKV()))

	stores := newTestStores(200)

	for _, store := range stores {
		c.Assert(s.cluster.putStore(store), IsNil)
	}
}

func (s *testGetStoresSuite) BenchmarkGetStores(c *C) {
	for i := 0; i < c.N; i++ {
		// Logic to benchmark
		s.cluster.core.Stores.GetStores()
	}
}
