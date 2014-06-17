package main

import (
	"encoding/json"
	"log"

	"github.com/eramus/worker"
)

const addResponseTube = `example_add_response`

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

	res.Data = a.A + a.B
	return
}

func main() {
	add := worker.NewWorker(addResponseTube, add, 1)
	add.Run()

	defer func() {
		f := make(chan struct{})
		add.Shutdown(f)
		<-f
	}()

	a := &addData{
		A: 2,
		B: 2,
	}

	req := make(map[string]interface{}, 2)
	req["request"] = "abc"
	req["data"] = a

	resp, err := worker.Send(addResponseTube, req, "abc")
	if err != nil {
		log.Println("err:", err)
		return
	}

	data := make(map[string]interface{}, 2)
	err = json.Unmarshal(resp, &data)
	if err != nil {
		log.Println("err:", err)
		return
	}

	log.Println("2 + 2 =", data["data"])
}
