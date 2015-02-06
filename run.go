package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/kr/beanstalk"
)

// Worker contains the exported parts of a worker. This allows
// the specifics about managing a worker to beleft up to the package.
type Worker interface {
	Run()
	Running() bool
	Shutdown(chan<- struct{})
}

type worker struct {
	tube       string
	workerFunc WorkerFunc
	count      int
	shutdown   chan struct{}
	control    control
	running    bool
}

type control struct {
	shutdown chan struct{}
	finished chan struct{}
	dead     chan struct{}
}

// NewWorker will return a Worker interface that can be used
// to control the underlying worker.
func NewWorker(tube string, workerFunc WorkerFunc, cnt int) Worker {
	if cnt < 1 {
		panic("need to create atleast one worker")
	}

	w := &worker{
		tube:       tube,
		workerFunc: workerFunc,
		count:      cnt,
		shutdown:   make(chan struct{}),
	}

	return w
}

// After a worker has been created, it can be started with the
// Run function. This will block until all of the worker instances
// have been started.
func (w *worker) Run() {
	if w.running {
		return
	}

	running := make(chan struct{})

	go func() {
		// control channels for workers
		w.control = control{
			shutdown: make(chan struct{}),
			finished: make(chan struct{}),
			dead:     make(chan struct{}),
		}

		// start up our workers
		for i := 0; i < w.count; i++ {
			go w.run()
		}

		// up and running
		close(running)

		// handle any dead workers or shutdown requests
		for {
			select {
			case <-w.control.dead:
				go w.run()
			case <-w.shutdown:
				return
			}
		}
	}()

	<-running
	w.running = true
	// and we're off
}

func (w *worker) run() {
	beanConn, err := beanstalk.Dial("tcp", beanstalkHost)
	if err != nil {
		panic(fmt.Sprintf("dial err: %s", err))
	}
	defer beanConn.Close()

	// catch a worker that has paniced
	defer func() {
		r := recover()
		if r != nil {
			log.Println("worker panic:", r)
			w.control.dead <- struct{}{}
		} else {
			w.control.finished <- struct{}{}
		}
		beanConn.Close()
	}()

	var req Request
	var watch = beanstalk.NewTubeSet(beanConn, getRequestTube(w.tube))

	for {
		// check for a shutdown signal
		select {
		case _, ok := <-w.control.shutdown:
			if !ok {
				return
			}
		default:
		}

		// get some work
		id, msg, err := watch.Reserve(defaultReserve)
		if err != nil {
			cerr, ok := err.(beanstalk.ConnError)
			if ok && cerr.Err == beanstalk.ErrTimeout {
				continue
			} else {
				panic(fmt.Sprintf("conn err: %s", err))
			}
		}

		// unmarshal the work payload
		err = json.Unmarshal(msg, &req)
		if err != nil {
			beanConn.Delete(id)
			panic(fmt.Sprintf("request json err: %s", err))
		}

		// send it off to our worker function
		req.jobId = id
		resp := w.workerFunc(&req)

		switch resp.Result {
		case Success:
			beanConn.Delete(id)
		case BuryJob:
			beanConn.Bury(id, 1)
			log.Printf("Burying job. Err: %s\n", resp.Error)
		case DeleteJob:
			beanConn.Delete(id)
			log.Printf("Deleting job. Err: %s\n", resp.Error)
		case ReleaseJob:
			releaseDelay := (time.Duration(resp.Delay) * time.Second)
			beanConn.Release(id, 1, releaseDelay)
			log.Printf("Releasing job for: %s Err: %s %s\n", releaseDelay.String(), resp.Error, string(msg))
		}

		// send back a response if requested
		if len(req.RequestId) > 0 && resp.Result != ReleaseJob {
			jsonRes, err := json.Marshal(resp)
			if err != nil {
				panic(fmt.Sprintf("response json err: %s", err))
			}

			beanConn.Tube.Name = getResponseTube(w.tube, req.RequestId)
			_, err = beanConn.Put(jsonRes, 0, 0, (3600 * time.Second))
			if err != nil {
				panic(fmt.Sprintf("write err: %s", err))
			}
		}
	}
}

// Running will return the current running status of a worker.
func (w *worker) Running() bool {
	return w.running
}

// Shutdown is a non-blocking function that will signal the worker
// instances and the Run routine to prepare to shutdown. Shutdown
// accepts an optional channel that serves as a notification that
// the shutdown has completed succesfully.
func (w *worker) Shutdown(finished chan<- struct{}) {
	if !w.running {
		return
	}
	// run this in a go routine so it doesnt block
	go func(f chan<- struct{}) {
		// close everything down and wait for checkins
		w.shutdown <- struct{}{}
		close(w.control.shutdown)

		for w.count > 0 {
			select {
			// check both in case someone
			// just now paniced
			case <-w.control.dead:
			case <-w.control.finished:
			}
			w.count--
		}
		w.running = false
		if f != nil {
			f <- struct{}{}
		}
	}(finished)
}
