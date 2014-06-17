package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

var (
	ErrBeanstalkConnect = errors.New("unable to connect to beanstalk")
	ErrJsonMarshal      = errors.New("json marshal")
	ErrUnableToSend     = errors.New("unable to send json")
	ErrNoResponse       = errors.New("did not receive a response")
)

// A function for determining the amount of delay that should be
// used each time a job is released.
type DelayDecay func(int) int

var defaultDecay = func(retries int) int {
	return retries
}

var defaultReserve = (2 * time.Second)

// The result of a job
type Result int

const (
	Success Result = iota
	BuryJob
	DeleteJob
	ReleaseJob
)

// The function that will be performed against a unit of work.
type WorkerFunc func(*Request) Response

// A container that describes the result of a consuming a unit
// of work.
type Response struct {
	Result Result      `json:"-"`
	Data   interface{} `json:"data"`
	Error  error       `json:"error"`
	Delay  int         `json:"-"`
}

// A unit of work that is passed to a WorkerFunc
type Request struct {
	jobId     uint64
	RequestId string          `json:"request"`
	Data      json.RawMessage `json:"data"`
}

// Helper function for retrying a job. This accepts an error and a
// number of times that a unit of work should be retried. An optional
// DelayDecay func is accepted for setting the amount of time, based
// on number of releases, that a unit of work should be delayed before
// acting against it again.
func (r *Request) RetryJob(err error, maxRetries int, delay DelayDecay) Response {
	if delay == nil {
		delay = defaultDecay
	}

	beanConn, dialErr := beanstalk.Dial("tcp", "0.0.0.0:11300")
	if dialErr != nil {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err,
			Delay:  delay(1),
		}
	}
	defer beanConn.Close()

	stats, statsErr := beanConn.StatsJob(r.jobId)
	if statsErr != nil {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err,
			Delay:  delay(1),
		}
	}

	_, ok := stats["releases"]
	if !ok {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err,
			Delay:  delay(1),
		}
	}

	releases, strErr := strconv.Atoi(stats["releases"])
	if strErr != nil {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err,
			Delay:  delay(1),
		}
	}

	if releases >= maxRetries {
		return r.BuryJob(err)
	}

	return Response{
		Result: ReleaseJob,
		Error:  err,
		Delay:  delay(releases),
	}
}

// Helper for burying a job.
func (r *Request) BuryJob(err error) Response {
	return Response{
		Result: BuryJob,
		Error:  err,
	}
}

// Helper for deleting a job.
func (r *Request) DeleteJob(err error) Response {
	return Response{
		Result: DeleteJob,
		Error:  err,
	}
}

// helpers for getting communication tubes
func getRequestTube(workerTube string) string {
	return fmt.Sprintf("%s_request", workerTube)
}

func getResponseTube(workerTube string, requestId string) string {
	return fmt.Sprintf("%s_%s_response", workerTube, requestId)
}

// An interface for generating request ids. This is needed
// when dealing with workers that act synchronously.
type RequestIdGenerator interface {
	GetRequestId() (string, error)
}

var requestIdGenerator RequestIdGenerator

// Helper for setting the RequestIdGenerator for a, or a collection
// of, worker
func SetRequestIdGenerator(gen RequestIdGenerator) {
	requestIdGenerator = gen
}

// Helper for getting a request id that can be used for
// returning finished work responses.
func GetRequestId() (string, error) {
	return requestIdGenerator.GetRequestId()
}
