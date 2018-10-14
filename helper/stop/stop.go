package stop

import (
	"context"
	"sync"
)

type Interface interface {
	Start()
	Stop()
}

// Struct is abstract class with Start/Stop methods
type Struct struct {
	sync.RWMutex
	// exit      chan struct{}
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup
	Go        func(callable func(ctx context.Context))
	WithCtx   func(callable func(ctx context.Context))
}

// Start ...
func (s *Struct) Start() {
	s.StartFunc(func() error { return nil })
}

// Stop ...
func (s *Struct) Stop() {
	s.StopFunc(func() {})
}

// StartFunc ...
func (s *Struct) StartFunc(startProcedure func() error) error {
	s.Lock()
	defer s.Unlock()

	// already started
	if s.ctx != nil {
		return nil
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	s.ctx, s.ctxCancel = ctx, ctxCancel
	s.Go = func(callable func(ctx context.Context)) {
		s.wg.Add(1)
		go func() {
			callable(ctx)
			s.wg.Done()
		}()
	}
	s.WithCtx = func(callable func(ctx context.Context)) {
		callable(ctx)
	}

	err := startProcedure()

	// stop all if start failed
	if err != nil {
		s.doStop(func() {})
	}

	return err
}

func (s *Struct) doStop(callable func()) {
	// already stopped
	if s.ctx == nil {
		return
	}

	s.ctxCancel()
	callable()
	s.wg.Wait()
	s.ctx = nil
	s.Go = func(callable func(ctx context.Context)) {}
	s.WithCtx = func(callable func(ctx context.Context)) {
		callable(context.TODO())
	}
}

// StopFunc ...
func (s *Struct) StopFunc(callable func()) {
	s.Lock()
	defer s.Unlock()

	// already stopped
	if s.ctx == nil {
		return
	}

	s.doStop(callable)
}
