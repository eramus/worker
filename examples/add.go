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
	add := worker.NewWorker(addTube, add, 1)
	add.Run()

	// shutdown the worker on exit
	defer func() {
		f := make(chan struct{})
		add.Shutdown(f)
		<-f
	}()

	// create a unit of work
	a := &addData{
		A: 2,
		B: 2,
	}

	// send it to our worker
	_, err := worker.Send(addTube, a, false)
	if err != nil {
		log.Println("err:", err)
	}
}
