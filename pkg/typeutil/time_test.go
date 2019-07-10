package typeutil

import (
	"math/rand"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTimeSuite{})

type testTimeSuite struct{}

func (s *testTimeSuite) TestParseTimestap(c *C) {
	for i := 0; i < 3; i++ {
		t := time.Now().Add(time.Second * time.Duration(rand.Int31n(1000)))
		data := Uint64ToBytes(uint64(t.UnixNano()))
		nt, err := ParseTimestamp(data)
		c.Assert(err, IsNil)
		c.Assert(nt.Equal(t), IsTrue)
	}
	data := []byte("pd")
	nt, err := ParseTimestamp(data)
	c.Assert(err, NotNil)
	c.Assert(nt.Equal(ZeroTime), IsTrue)
}

func (s *testTimeSuite) TestSubTimeByWallClock(c *C) {
	for i := 0; i < 3; i++ {
		r := rand.Int31n(1000)
		t1 := time.Now()
		t2 := t1.Add(time.Second * time.Duration(r))
		duration := SubTimeByWallClock(t2, t1)
		c.Assert(duration, Equals, time.Second*time.Duration(r))
	}
}
