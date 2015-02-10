package worker

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

// Send a unit of work to a worker. 'workerTube' determines the
// tube that will respond to incoming work. 'feedback' is lets the caller
// determine if a response is expected. 'options' is an optional parameter
// to configure the beanstalkd interaction, otherwise, the default options
// will be used.
func Send(tube string, data interface{}, feedback bool, options *Options) ([]byte, error) {
	if options == nil {
		options = defaultOptions
	}

	// put together our request data
	req := &struct {
		Data     interface{} `json:"data"`
		Feedback bool        `json:"feedback"`
	}{
		Data:     data,
		Feedback: feedback,
	}

	// marshal the data into a payload
	jsonReq, err := json.Marshal(req)
	if err != nil {
		return nil, ErrJSONMarshal
	}

	// connect to beanstalkd
	beanConn, err := beanstalk.Dial("tcp", options.Host)
	if err != nil {
		return nil, ErrBeanstalkConnect
	}
	defer beanConn.Close()

	// configure conn for send tube
	workerTube := beanstalk.Tube{beanConn, tube}

	// send it
	jobID, err := workerTube.Put(jsonReq, options.Priority, options.Delay, options.TTR)
	if err != nil {
		return nil, ErrUnableToSend
	}

	// no response -- all done with the send
	if !feedback {
		return nil, nil
	}

	var (
		resTube = tube + "_" + strconv.FormatUint(jobID, 10)
		watch   = beanstalk.NewTubeSet(beanConn, resTube)
		id      uint64
		msg     []byte
		start   = time.Now()
	)

	// wait for a response from the worker
	for {
		id, msg, err = watch.Reserve(options.Reserve)
		if err != nil {
			cerr, ok := err.(beanstalk.ConnError)
			if ok && cerr.Err == beanstalk.ErrTimeout {
				if time.Since(start) > options.Wait {
					return nil, ErrNoResponse
				}
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
