package main

import (
	"encoding/json"
	"log"

	"worker"
)

const firstTube = `example_add_first`
const secondTube = `example_add_second`

type firstData struct {
	A int `json:"a"`
}

type secondData struct {
	A int `json:"a"`
	B int `json:"b"`
}

var first = func(req *worker.Request) (res worker.Response) {
	f := &firstData{}
	err := json.Unmarshal(req.Data, f)
	if err != nil {
		return req.RetryJob(err, 3, nil)
	}

	s := &secondData{
		A: f.A,
		B: 2,
	}

	secondReq := make(map[string]interface{}, 2)
	secondReq["request"] = "second"
	secondReq["data"] = s

	resp, err := worker.Send(secondTube, secondReq, "second")
	if err != nil {
		log.Println("err:", err)
	}

	data := make(map[string]interface{}, 2)
	err = json.Unmarshal(resp, &data)
	if err != nil {
		log.Println("err:", err)
		return
	}

	res.Data = data["data"]
	return
}

var second = func(req *worker.Request) (res worker.Response) {
	s := &secondData{}
	err := json.Unmarshal(req.Data, s)
	if err != nil {
		return req.RetryJob(err, 3, nil)
	}

	res.Data = s.A + s.B
	return
}

func main() {
	var (
		shutdown = make(chan struct{})
		finished = make(chan struct{})
	)

	go worker.Run(worker.Worker{
		Tube:     firstTube,
		Work:     first,
		Count:    1,
		Shutdown: shutdown,
		Finished: finished,
	})

	go worker.Run(worker.Worker{
		Tube:     secondTube,
		Work:     second,
		Count:    1,
		Shutdown: shutdown,
		Finished: finished,
	})

	defer func() {
		close(shutdown)
		<-finished
		<-finished
	}()

	a := &firstData{
		A: 2,
	}

	req := make(map[string]interface{}, 2)
	req["request"] = "first"
	req["data"] = a

	resp, err := worker.Send(firstTube, req, "first")
	if err != nil {
		log.Println("err:", err)
	}

	data := make(map[string]interface{}, 2)
	err = json.Unmarshal(resp, &data)
	if err != nil {
		log.Println("err:", err)
		return
	}

	log.Println("A + B =", data["data"])
}
