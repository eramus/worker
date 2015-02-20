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
//	GetConn() (Conn, error)
}

type beanstalkWorker struct {
	tube       string
	workerFunc Func
	options    *Options
	control    control
	running    bool
}

type Conn interface{
	Get() (Request, error)
//	Send(Request, []byte) error
	Close() error
}

type beanstalkConn struct {
	tube string
	conn *beanstalk.Conn
	options *Options
	incoming *beanstalk.TubeSet
}

// A container that describes the result of consuming a unit
// of work.
type Response struct {
	Result Result        `json:"-"`
	Data   interface{}   `json:"data"`
	Error  string        `json:"error"`
	Delay  time.Duration `json:"-"`
}

func (br *beanstalkRequest) Handle(res *Response) error {
/*		switch res.Result {
		case BuryJob:
			res.priority = 1
		case ReleaseJob:
			res.priority = 1
			res.delay = out.Delay
		default:
		}*/



	switch res.Result {
	case Success:
		return bc.conn.Delete(id)
	case BuryJob:
		log.Printf("Burying job. Id: %d\n", id)
		return bc.conn.Bury(id, 1)
	case DeleteJob:
		log.Printf("Deleting job. Id: %d\n", id)
		return bc.conn.Delete(id)
	case ReleaseJob:
		log.Printf("Releasing job for: %s Id: %d %s\n", res.Delay.String(), id)
		return bc.conn.Release(id, 1, res.Delay)
	}
	return ErrUnknownResult
}

func (br *beanstalkRequest) Finish(response Response) error {

}

func (bc *beanstalkConn) Get() (Request, error) {
//incoming

//	id, msg, err := bc.conn.Get()
	// get some work
	id, msg, err := bc.incoming.Reserve(bc.options.Reserve)
	if err != nil {
		cerr, ok := err.(beanstalk.ConnError)
		if ok && cerr.Err == beanstalk.ErrTimeout {
			return nil, nil
		} else {
			return nil, err
//			panic(fmt.Sprintf("conn err: %s", err))
		}
	}

	// unmarshal the work payload
	req := &beanstalkRequest{}
	err = json.Unmarshal(msg, req)
	if err != nil {
		bc.conn.Delete(id)
		return nil, ErrBadJob
	}
	req.id = id
	req.host = bc.options.Host

	return req, nil
}

func (bc *beanstalkConn) Send(job *Request, response []byte) error {
	bc.conn.Tube.Name = bc.tube + "_" + strconv.FormatUint(job.id, 10)
	_, err := bc.conn.Put(response, bc.options.Priority, bc.options.Delay, bc.options.TTR)
	if err != nil {
		return err
	}
	return nil
}

func (bc *beanstalkConn) Close() error {
	return bc.conn.Close()
}

type control struct {
	completed chan result
	errored   chan error
	shutdown  chan struct{}
	dead      chan struct{}
}

// NewWorker will return a Worker interface that can be used
// to control the underlying worker. If options is nil, the
// default beanstalkd options will be used.
// TODO: better option handling
func New(tube string, workerFunc Func, options *Options) Worker {
	if options == nil {
		options = defaultOptions
	}

//	w := &worker{
	w := &beanstalkWorker{
		tube:       tube,
		workerFunc: workerFunc,
		options:    options,
	}

	return w
}

func (w *beanstalkWorker) getConn() (Conn, error) {
	beanConn, err := beanstalk.Dial("tcp", w.options.Host)
	if err != nil {
		return nil, err
	}
//	beanConn.incoming = beanstalk.NewTubeSet(beanConn, w.tube)
	conn := &beanstalkConn{
		tube: w.tube,
		conn: beanConn,
		options: w.options,
		incoming: beanstalk.NewTubeSet(beanConn, w.tube),

	}
	return conn, nil
}

// After a worker has been created, it can be started with the
// Run function. This will block until all of the workers
// have been started.
func (w *beanstalkWorker) Run() {
	if w.running {
		return
	}

	running := make(chan struct{})

	w.control = control{
		completed: make(chan result),
		errored:   make(chan error),
		shutdown:  make(chan struct{}),
		dead:      make(chan struct{}),
	}

	go w.run(running)

	<-running
	w.running = true
	// and we're off
}

/*func (w *worker) sendFeedback(job *Request, jsonRes []byte) error {
// w.getConn
	beanConn, err := beanstalk.Dial("tcp", w.options.Host)
	if err != nil {
		return ErrBeanstalkConnect
	}
	defer beanConn.Close()
// /w.getConn

// SEND
	err = conn.Send(job, jsonRes)
	if err != nil {
		return err
	}
	return nil
	beanConn.Tube.Name = w.tube + "_" + strconv.FormatUint(job.id, 10)
	_, err = beanConn.Put(jsonRes, w.options.Priority, w.options.Delay, w.options.TTR)
	if err != nil {
		return err
	}
	return nil
// /SEND
}*/

func (w *beanstalkWorker) work(jobs <-chan Request, done chan<- struct{}) {
	// catch a worker that has paniced
	defer func() {
//		r := recover()
//		if r != nil {
/*			log.Println("worker panic:", r)
			w.control.dead <- struct{}{}*/
//		} else {
			done <- struct{}{}
//		}
	}()

	for {
		job, ok := <-jobs
		if !ok {
			return
		}

		out := w.workerFunc(&job)

/*
		res := result{
			result: out.Result,
			jobID:  job.id,
		}
*/

		err := conn.Handle(job.id, &out)
		if err != nil {
			w.control.errored <- err
			continue
		}


		// send back a response if requested
		if job.Feedback && out.Result != ReleaseJob {
			jsonRes, err := json.Marshal(out)
			if err != nil {
				w.control.errored <- err
				continue
//				panic(fmt.Sprintf("response json err: %s", err))
			}

		 	conn, err := w.getConn()
			if err != nil {
				w.control.errored <- err
				continue
			}

			err = conn.Send(job, jsonRes)
			conn.Close()
			if err != nil {
				w.control.errored <- err
				continue
			}
//	return nil

/*
			// send back a response
			err = w.sendFeedback(&job, jsonRes)
			if err != nil {
				panic(fmt.Sprintf("worker response err: %s", err))
			}*/
		}

		// send back the work results
		w.control.completed <- res
	}
}

func (w *beanstalkWorker) run(started chan<- struct{}) {
// w.GetConn
	conn, err := w.getConn()
	if err != nil {
		panic(fmt.Sprintf("dial err: %s", err))
	}

/*	beanConn, err := beanstalk.Dial("tcp", w.options.Host)
	if err != nil {
		panic(fmt.Sprintf("dial err: %s", err))
	}*/
// /w.GetConn

	// worker comm channels
	jobs := make(chan Request)
	done := make(chan struct{})

	defer func() {
		// close the conn
// c.Close
		conn.Close()
//		beanConn.Close()
// /c.Close
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
//	var watch = beanstalk.NewTubeSet(beanConn, w.tube)

	// off we go
	close(started)
	running := true
	jobCnt := 0

	for jobCnt > 0 || running {
		// check the control channels
		select {
		case err := <-w.control.errored:
			log.Printf("Job errored: %s\n", err)
			fallthrough
		case <-w.control.completed:
// c.Finish
			// a worker is finished -- handle it
//			err = conn.Handle(res)
/*			switch res.result {
			case Success:
				beanConn.Delete(res.jobID)
			case BuryJob:
				beanConn.Bury(res.jobID, res.priority)
				log.Printf("Burying job. Id: %d\n", res.jobID)
			case DeleteJob:
				beanConn.Delete(res.jobID)
				log.Printf("Deleting job. Id: %d\n", res.jobID)
			case ReleaseJob:
				beanConn.Release(res.jobID, res.priority, res.delay)
				log.Printf("Releasing job for: %s Id: %d %s\n", res.delay.String(), res.jobID)
			}*/
// /c.Finish
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

// c.Get
		req, err := conn.Get()
		if err != nil {
			panic(fmt.Sprintf("conn err: %s", err))
		} else if req == nil {
			continue
		}
/*		id, msg, err := conn.Get()
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
		job.host = w.options.Host*/
// /c.Get

		jobCnt++
		go func(j Request) {
			// send it off!
			jobs <- j
		}(job)
	}
}

// Running will return the current running status of a worker.
func (w *beanstalkWorker) Running() bool {
	return w.running
}

// Shutdown is a non-blocking function that will signal the worker
// instances and the Run routine to prepare to shutdown. Shutdown
// accepts an optional channel that serves as a notification that
// the shutdown has completed succesfully.
func (w *beanstalkWorker) Shutdown(finished chan<- struct{}) {
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
