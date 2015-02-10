package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

// Worker contains the exported parts of a worker. This allows
// the specifics about managing a worker to be left up to the package.
type Worker interface {
	Run()
	Running() bool
	Shutdown(chan<- struct{})
}

type worker struct {
	tube       string
	workerFunc WorkerFunc
	options    *Options
	control    control
	running    bool
}

type control struct {
	completed chan result
	shutdown  chan struct{}
	dead      chan struct{}
}

// NewWorker will return a Worker interface that can be used
// to control the underlying worker. If options is nil, the
// default beanstalkd options will be used.
// TODO: better option handling
func NewWorker(tube string, workerFunc WorkerFunc, options *Options) Worker {
	if options == nil {
		options = defaultOptions
	}

	w := &worker{
		tube:       tube,
		workerFunc: workerFunc,
		options:    options,
	}

	return w
}

// After a worker has been created, it can be started with the
// Run function. This will block until all of the workers
// have been started.
func (w *worker) Run() {
	if w.running {
		return
	}

	running := make(chan struct{})

	w.control = control{
		completed: make(chan result),
		shutdown:  make(chan struct{}),
		dead:      make(chan struct{}),
	}

	go w.run(running)

	<-running
	w.running = true
	// and we're off
}

func (w *worker) sendFeedback(job *Request, jsonRes []byte) error {
	beanConn, err := beanstalk.Dial("tcp", w.options.Host)
	if err != nil {
		return ErrBeanstalkConnect
	}
	defer beanConn.Close()

	beanConn.Tube.Name = w.tube + "_" + strconv.FormatUint(job.id, 10)
	_, err = beanConn.Put(jsonRes, w.options.Priority, w.options.Delay, w.options.TTR)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) work(jobs <-chan Request, done chan<- struct{}) {
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
		job, ok := <-jobs
		if !ok {
			return
		}

		out := w.workerFunc(&job)

		res := result{
			result: out.Result,
			jobId:  job.id,
		}

		switch out.Result {
		case BuryJob:
			res.priority = 1
		case ReleaseJob:
			res.priority = 1
			res.delay = out.Delay
		default:
		}

		// send back a response if requested
		if job.Feedback && out.Result != ReleaseJob {
			jsonRes, err := json.Marshal(out)
			if err != nil {
				panic(fmt.Sprintf("response json err: %s", err))
			}

			// send back a response
			err = w.sendFeedback(&job, jsonRes)
			if err != nil {
				panic(fmt.Sprintf("worker response err: %s", err))
			}
		}

		// send back the work results
		w.control.completed <- res
	}
}

func (w *worker) run(started chan<- struct{}) {
	beanConn, err := beanstalk.Dial("tcp", w.options.Host)
	if err != nil {
		panic(fmt.Sprintf("dial err: %s", err))
	}

	// worker comm channels
	jobs := make(chan Request)
	done := make(chan struct{})

	defer func() {
		// close the conn
		beanConn.Close()
		// shutdown the workers
		close(jobs)
		// wait for them to stop
		for i := 0; i < w.options.Count; i++ {
			select {
			case <-done:
			case <-w.control.dead:
			}
		}
		close(w.control.shutdown)
	}()

	// start up our workers
	for i := 0; i < w.options.Count; i++ {
		go w.work(jobs, done)
	}

	// watch the worker tube
	var watch = beanstalk.NewTubeSet(beanConn, w.tube)

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
		default:
		}

		if !running {
			<-time.After(250 * time.Millisecond)
			continue
		}

		select {
		case <-w.control.dead:
			// a worker died -- start up a new one
			go w.work(jobs, done)
			continue
		case <-w.control.shutdown:
			// we need to shutdown
			running = false
			continue
		default:
		}

		// get some work
		id, msg, err := watch.Reserve(w.options.Reserve)
		if err != nil {
			cerr, ok := err.(beanstalk.ConnError)
			if ok && cerr.Err == beanstalk.ErrTimeout {
				continue
			} else {
				panic(fmt.Sprintf("conn err: %s", err))
			}
		}

		// unmarshal the work payload
		job := Request{}
		err = json.Unmarshal(msg, &job)
		if err != nil {
			beanConn.Delete(id)
			continue
		}
		job.id = id
		job.host = w.options.Host

		jobCnt++
		go func(j Request) {
			// send it off!
			jobs <- j
		}(job)
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
		w.control.shutdown <- struct{}{}
		<-w.control.shutdown

		w.running = false

		if f != nil {
			f <- struct{}{}
		}
	}(finished)
}
