package worker

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/kr/beanstalk"
)

// Send a unit of work to a worker. 'workerTube' determines the
// tube that will respond to incoming work. 'requestId' is an
// optional parameter for delivering responses back to the caller
func Send(tube string, data interface{}, feedback bool) ([]byte, error) {
	// put together our request data
	reqData := make(map[string]interface{}, 2)
	reqData["data"] = data
	if feedback {
		reqData["feedback"] = true
	}

	// marshal the data into a payload
	jsonReq, err := json.Marshal(reqData)
	if err != nil {
		return nil, ErrJsonMarshal
	}

	// connect to beanstalkd
	beanConn, err := beanstalk.Dial("tcp", beanstalkHost)
	if err != nil {
		return nil, ErrBeanstalkConnect
	}
	defer beanConn.Close()

	// configure conn for send tube
	workerTube := beanstalk.Tube{beanConn, getRequestTube(tube)}

	// send it
	jobId, err := workerTube.Put(jsonReq, 0, 0, (3600 * time.Second))
	if err != nil {
		return nil, ErrUnableToSend
	}

	// no response -- all done with the send
	if !feedback {
		return nil, nil
	}

	var (
		watch = beanstalk.NewTubeSet(beanConn, getResponseTube(tube, jobId))
		retry = 5
		id    uint64
		msg   []byte
	)

	// wait for a response from the worker
	for {
		id, msg, err = watch.Reserve(responsetime)
		if err != nil {
			cerr, ok := err.(beanstalk.ConnError)
			if ok && cerr.Err == beanstalk.ErrTimeout {
				if retry == 0 {
					return nil, ErrNoResponse
				}
				retry--
				continue
			} else {
				return nil, err
			}
		}
		break
	}

	// delete the job
	beanConn.Delete(id)

	// handle the response
	resp := &struct {
		Error string          `json:"error"`
		Data  json.RawMessage `json:"data"`
	}{}

	err = json.Unmarshal(msg, resp)
	if err != nil {
		return nil, err
	} else if len(resp.Error) > 0 {
		return nil, errors.New(resp.Error)
	}

	// success!
	return resp.Data, nil
}
