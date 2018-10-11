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

package simulator

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTableKeySuite{})

type testTableKeySuite struct{}

func (t *testTableKeySuite) TestGenerateTableKeys(c *C) {
	tableCount := 3
	size := 10
	keys := generateTableKeys(tableCount, size)
	c.Assert(len(keys), Equals, size)

	for i := 1; i < len(keys); i++ {
		c.Assert(keys[i-1], Less, keys[i])
		splitKey := string(generateTiDBEncodedSplitKey([]byte(keys[i-1]), []byte(keys[i])))
		c.Assert(keys[i-1], Less, splitKey)
		c.Assert(splitKey, Less, keys[i])
	}
	// empty key
	startKey := []byte("")
	endKey := []byte{116, 128, 0, 0, 0, 0, 0, 0, 255, 1, 0, 0, 0, 0, 0, 0, 0, 248}
	splitKey := generateTiDBEncodedSplitKey(startKey, endKey)
	c.Assert(startKey, Less, splitKey)
	c.Assert(splitKey, Less, endKey)

}
