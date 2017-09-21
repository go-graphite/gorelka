package ticker

import (
	"time"
)

type ForcableTicker struct {
	interval time.Duration
	C        <-chan time.Time
	c        chan time.Time

	force chan struct{}
}

func NewForcableTicker(d time.Duration) *ForcableTicker {
	c := make(chan time.Time)
	t := &ForcableTicker{
		interval: d,
		force:    make(chan struct{}),
		C:        c,
		c:        c,
	}
	go t.ticker()
	return t
}

func (t *ForcableTicker) Force() {
	t.force <- struct{}{}
}

func (t *ForcableTicker) ticker() {
	internalTicker := time.NewTicker(t.interval)
	for {
		select {
		case <-t.force:
			internalTicker.Stop()
			t.c <- time.Now()
			internalTicker = time.NewTicker(t.interval)
		case now := <-internalTicker.C:
			t.c <- now
		}
	}
}
