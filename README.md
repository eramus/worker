[![GoDoc](https://godoc.org/github.com/eramus/worker?status.svg)](https://godoc.org/github.com/eramus/worker)
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

// a function that will accept work requests and process them
var add = func(req *worker.Request) (res worker.Response) {
	a := &addData{}
	err := json.Unmarshal(req.Data, a)
	if err != nil {
		return req.RetryJob(err, 3, nil)
	}

	log.Printf("%d + %d = %d", a.A, a.B, (a.A + a.B))
	return
}

// Typically, the Run and Send functions would not be called
// from the same process, but, for ease of demonstration, they
// are combined for this example.
func main() {
	// define and run a worker
	add := worker.New(addTube, add, nil)
	add.Run()

	// shutdown the worker on exit
	defer func() {
		f := make(chan struct{})
		add.Shutdown(f)
		<-f
	}()

	// create a unit of work
	a := addData{
		A: 2,
		B: 2,
	}

	// send it to our worker
	_, err := worker.Send(addTube, a, false, nil)
	if err != nil {
		log.Println("err:", err)
	}
}
```

## Package Overview ##

```go
type Worker interface {
	Run()
	Running() bool
	Shutdown(chan<- struct{})
}
```

This interface is used to hide the implementation of the base package from the caller. This exposes just enough functionality to keep working with and defining a worker simple. ```Run``` will block until all worker instances have been launched. ```Shutdown``` does not block so that multiple workers can be shutdown at the same time. ```Shutdown``` accepts an optional channel that can be used to signal the main caller that the shutdown was completed successfully.

```go
func New(tube string, workerFunc Func, options *Options) Worker
```

```New``` will return a ```Worker``` to the caller. Underneath it creates a ```worker``` to encapsulate the details and functionality for running a worker. ```tube``` will be the beanstalk tube that the worker will respond to. ```workerFunc``` is a function that will be called when work is delivered via ```tube```. ```options``` is an optional parameter for configuring the underlying beanstalkd connection and number of worker routines that will be spawned.

```go
type Func func(*Request) Response
```

The expected worker function definition. Your functions should accept a ```Request``` and return a ```Response``` -- easy as that. What you do inside is completely up to you. Take a look at the examples directory for some ideas.

```go
func Send(tube string, data interface{}, feedback bool, options *Options) ([]byte, error)
```

This can be used for sending work requests to your workers. Data is marshalled to a json string before being sent across beanstalk. ```feedback``` determines if this should return a response from the worker. ```options``` is an optional parameter for configuring the underlying beanstalkd connection. Raw json is returned for requests that expect responses.

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

## TODO ##

* tests!
