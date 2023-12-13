package hazelcasts

import (
	"github.com/aacfactory/fns/barriers"
	"github.com/aacfactory/fns/shareds"
)

var (
	extraShared               shareds.Shared          = nil
	extraSharedLockersBuilder shareds.LockersBuilder  = nil
	extraSharedStoreBuilder   shareds.StoreBuilder    = nil
	extraBarrier              barriers.BarrierBuilder = nil
)

func UseExtraShared(shared shareds.Shared) {
	extraShared = shared
}

func UseExtraSharedLockers(builder shareds.LockersBuilder) {
	extraSharedLockersBuilder = builder
}

func UseExtraSharedStore(builder shareds.StoreBuilder) {
	extraSharedStoreBuilder = builder
}

func UseExtraBarrier(builder barriers.BarrierBuilder) {
	extraBarrier = builder
}
