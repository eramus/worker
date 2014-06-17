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
