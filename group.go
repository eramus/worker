package worker

import (
	"sync"
)

// WorkerGroup is a protected map of workers. This also provides functions
// for starting and stopping groups of workers. This saves the trouble of
// needing to handle shutting down larges groups of workers.
type WorkerGroup interface {
	Add(string, Worker)
	Remove(string) Worker
	Run()
	Shutdown()
}

type workerGroup struct {
	sync.RWMutex
	workers map[string]Worker
}

// Return an initalized WorkerGroup for controlling workers as a group.
func NewWorkerGroup() WorkerGroup {
	wg := &workerGroup{
		workers: make(map[string]Worker),
	}
	return wg
}

// Add a worker to the group of workers. The name given must be unique to
// avoid a panic.
func (wg *workerGroup) Add(name string, worker Worker) {
	wg.Lock()
	defer wg.Unlock()

	_, ok := wg.workers[name]
	if ok {
		panic("worker name must be unique")
	}

	wg.workers[name] = worker
}

// Remove will remove the named worker from the group. The worker is
// returned if it is found.
func (wg *workerGroup) Remove(name string) Worker {
	wg.Lock()
	defer wg.Unlock()

	worker := wg.workers[name]
	delete(wg.workers, name)
	return worker
}

// Run the group of workers.
func (wg *workerGroup) Run() {
	wg.RLock()
	defer wg.RUnlock()

	// kick it all off
	for _, worker := range wg.workers {
		if worker.Running() {
			continue
		}
		worker.Run()
	}
}

// Shut down the group of workers
func (wg *workerGroup) Shutdown() {
	f := make(chan struct{})

	wg.Lock()
	defer wg.Unlock()

	// start shutting them down
	for name, worker := range wg.workers {
		worker.Shutdown(f)
		delete(wg.workers, name)
	}
	// wait for everyone to check in
	for i := 0; i < len(wg.workers); i++ {
		<-f
	}
}
