package worker

import "sync"

// group is a protected map of workers. This also provides functions
// for starting and stopping groups of workers. This saves the trouble of
// needing to handle starting and shutting down larges groups of workers.
type Group interface {
	Add(string, Func)
	Remove(string) Worker
	Run()
	Shutdown()
}

type group struct {
	sync.RWMutex
	workers map[string]Worker
	options *Options
}

// Return an initalized Group for controlling workers as a group.
// If options is nil, the default beanstalkd options will be used.
func NewGroup(options *Options) Group {
	if options == nil {
		options = defaultOptions
	}

	g := &group{
		workers: make(map[string]Worker),
		options: options,
	}

	return g
}

// Add a worker to the group of workers.
func (g *group) Add(tube string, workerFunc Func) {
	g.Lock()
	defer g.Unlock()

	_, ok := g.workers[tube]
	if ok {
		panic("worker name must be unique")
	}

	w := New(tube, workerFunc, g.options)
	g.workers[tube] = w
}

// Remove will remove the named worker from the group. The worker is
// returned if it is found. It is not shutdown.
func (g *group) Remove(tube string) Worker {
	g.Lock()
	defer g.Unlock()

	w := g.workers[tube]
	delete(g.workers, tube)
	return w
}

// Run the group of workers.
func (g *group) Run() {
	g.RLock()
	defer g.RUnlock()

	// kick it all off
	for _, w := range g.workers {
		if w.Running() {
			continue
		}
		w.Run()
	}
}

// Shut down the group of workers
func (g *group) Shutdown() {
	f := make(chan struct{})

	g.Lock()
	defer g.Unlock()

	// start shutting them down
	for _, w := range g.workers {
		w.Shutdown(f)
	}
	// wait for everyone to check in
	for i := 0; i < len(g.workers); i++ {
		<-f
	}
	for t := range g.workers {
		delete(g.workers, t)
	}
}
