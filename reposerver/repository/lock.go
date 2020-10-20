package repository

import (
	"io"
	"sync"

	ioutil "github.com/argoproj/gitops-engine/pkg/utils/io"
)

func NewRepositoryLock() *repositoryLock {
	return &repositoryLock{stateByKey: map[string]*repositoryState{}}
}

type repositoryLock struct {
	lock       sync.Mutex
	stateByKey map[string]*repositoryState
}

// Lock acquires lock unless lock is already acquired with the same commit and allowConcurrent is set to true
func (r *repositoryLock) Lock(key string, revision string, allowConcurrent bool, init func() error) (io.Closer, error) {
	for {
		r.lock.Lock()
		state, ok := r.stateByKey[key]
		if !ok {
			state = &repositoryState{cond: sync.NewCond(&sync.Mutex{})}
			r.stateByKey[key] = state
		}

		closer := ioutil.NewCloser(func() error {
			r.lock.Lock()
			defer r.lock.Unlock()
			state.processCount--
			if state.processCount == 0 {
				state.initialized = false
				state.revision = ""
				state.cond.Broadcast()
			}
			return nil
		})

		ensureInitialized := func() error {
			state.initSync.RLock()
			if state.initialized {
				state.initSync.RUnlock()
				return nil
			}
			state.initSync.RUnlock()
			state.initSync.Lock()
			defer state.initSync.Unlock()
			err := init()
			if err == nil {
				state.initialized = true
			}
			return err
		}

		if state.revision == "" {
			// no in progress operation for that repo. Go ahead.
			state.revision = revision
			state.processCount = 1
			r.lock.Unlock()
			if err := ensureInitialized(); err != nil {
				ioutil.Close(closer)
				return nil, err
			}
			return closer, nil
		} else if state.revision == revision && allowConcurrent {
			// same revision already processing and concurrent processing allowed. Increment process count and go ahead.
			state.processCount++
			r.lock.Unlock()
			if err := ensureInitialized(); err != nil {
				ioutil.Close(closer)
				return nil, err
			}
			return closer, nil
		} else {
			// wait when all in-flight processes of this revision complete and try again
			r.lock.Unlock()
			state.cond.L.Lock()
			state.cond.Wait()
		}
	}
}

type repositoryState struct {
	cond         *sync.Cond
	revision     string
	processCount int
	initSync     sync.RWMutex
	initialized  bool
}
