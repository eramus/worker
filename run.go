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
	completed chan result
	shutdown  chan struct{}
	finished  chan struct{}
	dead      chan struct{}
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

	w.control = control{
		completed: make(chan result),
		shutdown:  make(chan struct{}),
		finished:  make(chan struct{}),
		dead:      make(chan struct{}),
	}

	go w.run(running)

	<-running
	w.running = true
	// and we're off
}

func (w *worker) sendFeedback(job *Request, jsonRes []byte) error {
	beanConn, err := beanstalk.Dial("tcp", beanstalkHost)
	if err != nil {
		return err
	}
	defer beanConn.Close()

	beanConn.Tube.Name = getResponseTube(w.tube, job.id)
	_, err = beanConn.Put(jsonRes, 0, 0, (3600 * time.Second))
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) work(jobs <-chan *Request, done chan<- struct{}) {
	// catch a worker that has paniced
	defer func() {
		r := recover()
		if r != nil {
			log.Println("worker panic:", r)
			w.control.dead <- struct{}{}
		} else {
			done <- struct{}{}
		}
	}()

	for {
		select {
		case job, ok := <-jobs:
			if !ok && job == nil {
				return
			}

			out := w.workerFunc(job)

			res := result{
				result: out.Result,
				jobId:  job.id,
			}

			switch out.Result {
			case BuryJob:
				res.priority = 1
			case ReleaseJob:
				res.priority = 1
				res.delay = time.Duration(out.Delay) * time.Second
			default:
			}

			w.control.completed <- res

			// send back a response if requested
			if job.Feedback && out.Result != ReleaseJob {
				jsonRes, err := json.Marshal(out)
				if err != nil {
					panic(fmt.Sprintf("response json err: %s", err))
				}

				// send back a response
				err = w.sendFeedback(job, jsonRes)
				if err != nil {
					panic(fmt.Sprintf("worker response err: %s", err))
				}
			}
		}
	}
}

func (w *worker) run(started chan<- struct{}) {
	beanConn, err := beanstalk.Dial("tcp", beanstalkHost)
	if err != nil {
		panic(fmt.Sprintf("dial err: %s", err))
	}
	defer beanConn.Close()

	// worker comm channels
	jobs := make(chan *Request)
	done := make(chan struct{})

	defer func() {
		// shutdown the workers
		close(jobs)
		// wait for them to stop
		for i := 0; i < w.count; i++ {
			select {
			case <-done:
			case <-w.control.dead:
			}
		}
		w.control.finished <- struct{}{}
	}()

	// start up our workers
	for i := 0; i < w.count; i++ {
		go w.work(jobs, done)
	}

	// watch the worker tube
	var watch = beanstalk.NewTubeSet(beanConn, getRequestTube(w.tube))

	// off we go
	close(started)
	running := true
	jobCnt := 0

	for jobCnt > 0 || running {
		// check the control channels
		select {
		case res := <-w.control.completed:
			// a worker is finished -- handle it
			switch res.result {
			case Success:
				beanConn.Delete(res.jobId)
			case BuryJob:
				beanConn.Bury(res.jobId, res.priority)
				log.Printf("Burying job. Id: %d\n", res.jobId)
			case DeleteJob:
				beanConn.Delete(res.jobId)
				log.Printf("Deleting job. Id: %d\n", res.jobId)
			case ReleaseJob:
				beanConn.Release(res.jobId, res.priority, res.delay)
				log.Printf("Releasing job for: %s Id: %d %s\n", res.delay.String(), res.jobId)
			}
			jobCnt--
		case <-w.control.dead:
			// a worker died -- start up a new one
			go w.work(jobs, done)
			continue
		case _, ok := <-w.control.shutdown:
			if !ok {
				if !running {
					<-time.After(1 * time.Millisecond)
					continue
				}
				// we need to shutdown
				running = false
				continue
			}
		default:
		}

		// get some work
		id, msg, err := watch.Reserve(reserveTime)
		if err != nil {
			cerr, ok := err.(beanstalk.ConnError)
			if ok && cerr.Err == beanstalk.ErrTimeout {
				continue
			} else {
				panic(fmt.Sprintf("conn err: %s", err))
			}
		}

		job := &Request{}
		// unmarshal the work payload
		err = json.Unmarshal(msg, job)
		if err != nil {
			beanConn.Delete(id)
			continue
		}
		job.id = id

		jobCnt++
		go func() {
			// send it off!
			jobs <- job
		}()
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
		close(w.control.shutdown)

		<-w.control.finished
		w.running = false

		if f != nil {
			f <- struct{}{}
		}
	}(finished)
}
