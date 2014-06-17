package main

import (
	"encoding/json"
	"log"

	"github.com/eramus/worker"
)

const addTube = `example_add`

type addData struct {
	A int `json:"a"`
	B int `json:"b"`
}

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
	var (
		shutdown = make(chan struct{})
		finished = make(chan struct{})
	)

	go worker.Run(worker.Worker{
		Tube:     addTube,
		Work:     add,
		Count:    1,
		Shutdown: shutdown,
		Finished: finished,
	})

	defer func() {
		close(shutdown)
		<-finished
	}()

	a := &addData{
		A: 2,
		B: 2,
	}

	req := make(map[string]interface{}, 2)
	req["data"] = a

	_, err := worker.Send(addTube, req, "")
	if err != nil {
		log.Println("err:", err)
	}
}
