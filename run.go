package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/kr/beanstalk"
)

type control struct {
	shutdown chan struct{}
	finished chan struct{}
	dead     chan struct{}
}

// The heart of this package. 'Run' accepts a single
// argument, 'worker'. This defines which tube should respond
// to request, what function to act against incoming work
// and also a pair of channels for shutting down gracefully.
func Run(worker Worker) {
	// control channels for workers
	control := control{
		shutdown: make(chan struct{}),
		finished: make(chan struct{}),
		dead:     make(chan struct{}),
	}

	// start up our workers
	for i := 0; i < worker.Count; i++ {
		go run(worker.Tube, worker.Work, control)
	}

	var (
		running = true
		ok      bool
	)

	// handle any dead workers or shutdown requests
	for running {
		select {
		case <-control.dead:
			go run(worker.Tube, worker.Work, control)
		case _, ok = <-worker.Shutdown:
			if !ok {
				running = false
			}
		}
	}

	// close everything down and wait for checkins
	close(control.shutdown)
	for i := worker.Count; i > 0; i-- {
		select {
		case <-control.dead:
		case <-control.finished:
		}
	}

	// signal main that we are done
	worker.Finished <- struct{}{}
}

func run(workerTube string, workerFunc WorkerFunc, control control) {
	beanConn, err := beanstalk.Dial("tcp", "0.0.0.0:11300")
	if err != nil {
		panic(fmt.Sprintf("dial err: %s", err))
	}
	defer beanConn.Close()

	// catch a worker that has paniced
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println("worker panic:", r)
			control.dead <- struct{}{}
		} else {
			control.finished <- struct{}{}
		}
		beanConn.Close()
	}()

	var req Request
	var watch = beanstalk.NewTubeSet(beanConn, getRequestTube(workerTube))

	for {
		// check for a shutdown signal
		select {
		case _, ok := <-control.shutdown:
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
		response := workerFunc(&req)

		switch response.Result {
		case Success:
			beanConn.Delete(id)
		case BuryJob:
			beanConn.Bury(id, 1)
			log.Printf("Burying job. Err: %s\n", response.Error)
		case DeleteJob:
			beanConn.Delete(id)
			log.Printf("Deleting job. Err: %s\n", response.Error)
		case ReleaseJob:
			releaseDelay := (time.Duration(response.Delay) * time.Second)
			beanConn.Release(id, 1, releaseDelay)
			log.Printf("Releasing job for: %s Err: %s %s\n", releaseDelay.String(), response.Error, string(msg))
		}

		// send back a response if requested
		if req.RequestId != "" && response.Result != ReleaseJob {
			jsonRes, err := json.Marshal(response)
			if err != nil {
				panic(fmt.Sprintf("response json err: %s", err))
			}

			beanConn.Tube.Name = getResponseTube(workerTube, req.RequestId)
			_, err = beanConn.Put(jsonRes, 0, 0, (3600 * time.Second))
			if err != nil {
				panic(fmt.Sprintf("write err: %s", err))
			}
		}
	}
}
