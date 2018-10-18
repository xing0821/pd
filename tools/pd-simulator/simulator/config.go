package simulator

import (
	"time"

	"github.com/pingcap/pd/pkg/typeutil"
	"github.com/pingcap/pd/server"
)

const (
	// tick
	defaultSimTickInterval = 100 * time.Millisecond
	// store
	defaultStoreCapacityGB    = 1024
	defaultStoreAvailableGB   = 1024
	defaultStoreIOMBPerSecond = 40
	defaultStoreVersion       = "2.1.0"
	// server
	defaultLeaderLease                 = 1
	defaultTsoSaveInterval             = 200 * time.Millisecond
	defaultTickInterval                = 100 * time.Millisecond
	defaultElectionInterval            = 3 * time.Second
	defaultLeaderPriorityCheckInterval = 100 * time.Millisecond
)

// SimConfig is the simulator configuration.
type SimConfig struct {
	// tick
	SimTickInterval typeutil.Duration `toml:"sim-tick-interval"`
	// store
	StoreCapacityGB    uint64 `toml:"store-capacity"`
	StoreAvailableGB   uint64 `toml:"store-available"`
	StoreIOMBPerSecond int64  `toml:"store-io-per-second"`
	StoreVersion       string `toml:"store-version"`
	// schedule
	Schedule server.ScheduleConfig `toml:"schedule"`
	// server
	LeaderLease                 int64             `toml:"lease"`
	TsoSaveInterval             typeutil.Duration `toml:"tso-save-interval"`
	TickInterval                typeutil.Duration `toml:"tick-interval"`
	ElectionInterval            typeutil.Duration `toml:"election-interval"`
	LeaderPriorityCheckInterval typeutil.Duration `toml:"leader-priority-check-interval"`
}

// NewSimConfig create a new configuration of the simulator.
func NewSimConfig() *SimConfig {
	return &SimConfig{}
}

func adjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration == 0 {
		v.Duration = defValue
	}
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

// Adjust is used to adjust configurations
func (sc *SimConfig) Adjust() {
	adjustDuration(&sc.SimTickInterval, defaultSimTickInterval)
	adjustUint64(&sc.StoreCapacityGB, defaultStoreCapacityGB)
	adjustUint64(&sc.StoreAvailableGB, defaultStoreAvailableGB)
	adjustInt64(&sc.StoreIOMBPerSecond, defaultStoreIOMBPerSecond)
	adjustString(&sc.StoreVersion, defaultStoreVersion)
	adjustInt64(&sc.LeaderLease, defaultLeaderLease)
	adjustDuration(&sc.TsoSaveInterval, defaultTsoSaveInterval)
	adjustDuration(&sc.TickInterval, defaultTickInterval)
	adjustDuration(&sc.ElectionInterval, defaultElectionInterval)
	adjustDuration(&sc.LeaderPriorityCheckInterval, defaultLeaderPriorityCheckInterval)
}
