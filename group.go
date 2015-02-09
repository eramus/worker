package worker

import "sync"

// WorkerGroup is a protected map of workers. This also provides functions
// for starting and stopping groups of workers. This saves the trouble of
// needing to handle starting and shutting down larges groups of workers.
type WorkerGroup interface {
	Add(string, WorkerFunc)
	Remove(string) Worker
	Run()
	Shutdown()
}

type workerGroup struct {
	sync.RWMutex
	workers map[string]Worker
	options *Options
}

// Return an initalized WorkerGroup for controlling workers as a group.
// If options is nil, the default beanstalkd options will be used.
func NewWorkerGroup(options *Options) WorkerGroup {
	if options == nil {
		options = defaultOptions
	}

	wg := &workerGroup{
		workers: make(map[string]Worker),
		options: options,
	}

	return wg
}

// Add a worker to the group of workers.
func (wg *workerGroup) Add(tube string, workerFunc WorkerFunc) {
	wg.Lock()
	defer wg.Unlock()

	_, ok := wg.workers[tube]
	if ok {
		panic("worker name must be unique")
	}

	w := NewWorker(tube, workerFunc, wg.options)
	wg.workers[tube] = w
}

// Remove will remove the named worker from the group. The worker is
// returned if it is found. It is not shutdown.
func (wg *workerGroup) Remove(tube string) Worker {
	wg.Lock()
	defer wg.Unlock()

	w := wg.workers[tube]
	delete(wg.workers, tube)
	return w
}

// Run the group of workers.
func (wg *workerGroup) Run() {
	wg.RLock()
	defer wg.RUnlock()

	// kick it all off
	for _, w := range wg.workers {
		if w.Running() {
			continue
		}
		w.Run()
	}
}

// Shut down the group of workers
func (wg *workerGroup) Shutdown() {
	f := make(chan struct{})

	wg.Lock()
	defer wg.Unlock()

	// start shutting them down
	for _, w := range wg.workers {
		w.Shutdown(f)
	}
	// wait for everyone to check in
	for i := 0; i < len(wg.workers); i++ {
		<-f
	}
	for t, _ := range wg.workers {
		delete(wg.workers, t)
	}
}
