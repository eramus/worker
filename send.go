package worker

import (
	"encoding/json"
	"time"

	"github.com/kr/beanstalk"
)

// Send a unit of work to a worker. 'workerTube' determines the
// tube that will respond to incoming work. 'requestId' is an
// optional parameter for delivering responses back to the caller
func Send(workerTube string, data map[string]interface{}, requestId string) ([]byte, error) {
	beanConn, err := beanstalk.Dial("tcp", "0.0.0.0:11300")
	if err != nil {
		return nil, ErrBeanstalkConnect
	}
	defer beanConn.Close()

	// marshal the data into a payload
	jsonReq, err := json.Marshal(data)
	if err != nil {
		return nil, ErrJsonMarshal
	}

	// configure conn for send tube
	tube := beanstalk.Tube{beanConn, getRequestTube(workerTube)}

	// send it
	_, err = tube.Put(jsonReq, 0, 0, (3600 * time.Second))
	if err != nil {
		return nil, ErrUnableToSend
	}

	// all done with the send
	if requestId == "" {
		return nil, nil
	}

	var (
		watch = beanstalk.NewTubeSet(beanConn, getResponseTube(workerTube, requestId))
		retry = 5
		id    uint64
		msg   []byte
	)

	// wait for a response from the worker
	for {
		id, msg, err = watch.Reserve(defaultReserve)
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

	beanConn.Delete(id)
	return msg, nil
}
