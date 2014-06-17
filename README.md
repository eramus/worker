# Go Beanstalkd Worker #

This package provides a collection of helpers for easily designing and scaling workers in your infrastructure. A Worker is function that performs work, and it is able to live on any number of systems. Beanstalkd acts as common transport layer for incoming work requests and outgoing work responses.

## Installation ##

    # install the library:
    go get github.com/eramus/worker

    // use in your .go code:
    import (
        "github.com/eramus/worker"
    )

## Quickstart ##

```go
package main

import (
	"encoding/json"
	"log"

	"github.com/eramus/worker"
)

// a beanstalk tube used to deliver work requests
const addTube = `example_add`

// a container to unmarshal incoming data
type addData struct {
	A int `json:"a"`
	B int `json:"b"`
}

// a function that will accepts work requests and process them
var add = func(req *worker.Request) (res worker.Response) {
	a := &addData{}
	err := json.Unmarshal(req.Data, a)
	if err != nil {
		return req.RetryJob(err, 3, nil)
	}

	log.Printf("%d + %d = %d", a.A, a.B, (a.A + a.B))
	return
}

func main() {
	// global shutdown channels
	var (
		shutdown = make(chan struct{})
		finished = make(chan struct{})
	)

	// define and launch a worker
	go worker.Run(worker.Worker{
		Tube:     addTube,
		Work:     add,
		Count:    1,
		Shutdown: shutdown,
		Finished: finished,
	})

	// shutdown the worker on exit
	defer func() {
		close(shutdown)
		<-finished
	}()

	// create a unit of work
	a := &addData{
		A: 2,
		B: 2,
	}

	req := make(map[string]interface{}, 2)
	req["data"] = a

	// send it to our worker
	_, err := worker.Send(addTube, req, "")
	if err != nil {
		log.Println("err:", err)
	}
}
```

## Package Overview ##

```go
func Run(worker Worker)
```

This helper handles configuring and running your worker. ```Run``` will respond to unhandled worker panics by restarting the working. Care should be taken in this regard, because a worker may spin out of control if properly handling is implemented by the user of the package. ```Run``` is also responsible for gracefully shutting down workers when the main process is ready to exit.

```go
type WorkerFunc func(*Request) Response
```

The expected worker function definition. Your functions should accept a ```Request``` and return a ```Response``` -- easy as that. What you do inside is completely up to you. Take a look at the examples directory for some ideas.

```go
func Send(workerTube string, data map[string]interface{}, requestId string) ([]byte, error)
```

This can be used for sending work requests to your workers. Data is marshalled to a json string before being sent across beanstalk. The ```requestId``` is an optional parameter that signals ```Send``` that a response from the worker is expected. Raw json is returned for requests that expect responses.

```go
type DelayDecay func(int) int
```

This allows you to define a function that can be used to adjust the amount of time in between retries in the event there is a failure in your worker.

```go
func (r *Request) RetryJob(err error, maxRetries int, delay DelayDecay) Response
```

```RetryJob``` is a helper function that can be used inside of your worker functions to signal ```Run``` to retry a work request. ```RetryJob``` will call ```BuryJob``` if the max number of retries has been reached. If a nil ```DelayDecay``` parameter is passed in, the default decay function will be used (+1 second per retry).

```go
func (r *Request) BuryJob(err error) Response
```

```BuryJob``` can be used to bury work requests that have failed but should not be deleted. These can be examined a later time to better tune your worker functions.

```go
func (r *Request) DeleteJob(err error) Response
```

```DeleteJob``` is used for cases that a work request should just be deleted on error.

```go
type RequestIdGenerator interface {
	GetRequestId() (string, error)
}
```

An interface for defining a way to generate request ids for work requests that expect a response. This could be a simple increment, a UUID generator or something more complex involving an outside service. I like to use noeqd (https://github.com/noeq/noeqd).

## TODO ##

* Better connection handling to beanstalkd. An internal connection pool perhaps?
* A default request id generator
* More flexibility for sending work requests: fire-and-forget, response handlers, etc.
* tests!
