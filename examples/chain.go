package main

import (
	"encoding/json"
	"log"

	"github.com/eramus/worker"
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

	resp, err := worker.Send(secondTube, s, true)
	if err != nil {
		log.Println("err:", err)
	}

	var data int
	err = json.Unmarshal(resp, &data)
	if err != nil {
		log.Println("err:", err)
		return
	}

	res.Data = data
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
	wg := worker.NewWorkerGroup()
	wg.Add("first", worker.NewWorker(firstTube, first, 1))
	wg.Add("second", worker.NewWorker(secondTube, second, 1))

	wg.Run()
	defer wg.Shutdown()

	a := &firstData{
		A: 2,
	}

	resp, err := worker.Send(firstTube, a, true)
	if err != nil {
		log.Println("err:", err)
		return
	}

	var data int
	err = json.Unmarshal(resp, &data)
	if err != nil {
		log.Println("err:", err)
		return
	}

	log.Println("2 + 2 =", data)
}
