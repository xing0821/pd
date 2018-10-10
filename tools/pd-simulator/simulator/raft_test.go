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

package simulator

import (
	"bytes"
	"strings"
	"testing"
)

func TestGenerateTableKeys(t *testing.T) {
	tableCount := 3
	size := 10
	keys := generateTableKeys(tableCount, size)
	if len(keys) != size {
		t.Fatalf("keys length %d, expected %d", len(keys), size)
	}
	for i := 1; i < len(keys); i++ {
		if strings.Compare(keys[i-1], keys[i]) >= 0 {
			t.Fatal("not an increamental sequence")
		}
		splitKey := string(generateMvccSplitKey([]byte(keys[i-1]), []byte(keys[i])))
		if strings.Compare(keys[i-1], splitKey) >= 0 {
			t.Fatalf("not expected split key")
		}
		if strings.Compare(splitKey, keys[i]) >= 0 {
			t.Fatalf("not expected split key")
		}
	}
	// empty key
	startKey := []byte("")
	endKey := []byte{116, 128, 0, 0, 0, 0, 0, 0, 255, 1, 0, 0, 0, 0, 0, 0, 0, 248}
	splitKey := generateMvccSplitKey(startKey, endKey)
	if bytes.Compare(startKey, splitKey) >= 0 {
		t.Fatalf("not expected split key")
	}
	if bytes.Compare(splitKey, endKey) >= 0 {
		t.Fatalf("not expected split key")
	}
}
