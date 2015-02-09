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

	defaultHost    = "0.0.0.0:11300"
	defaultReserve = (2 * time.Second)
	defaultDelay   = time.Duration(0)
	defaultTTR     = (3600 * time.Second)
)

// Common beanstalkd options that are used by
// this package.
type Options struct {
	Host     string
	Count    int
	Reserve  time.Duration
	Priority uint32
	Delay    time.Duration
	TTR      time.Duration
}

var defaultOptions = &Options{
	Host:     defaultHost,
	Count:    1,
	Reserve:  defaultReserve,
	Priority: 0,
	Delay:    defaultDelay,
	TTR:      defaultTTR,
}

// Get a copy of the default options.
func GetDefaults() Options {
	return *defaultOptions
}

// A function for determining the amount of delay that should be
// used each time a job is released.
type DelayDecay func(int) int

var defaultDecay = func(retries int) int {
	return 1
}

// The result of a job
type Result int

const (
	Success Result = iota
	BuryJob
	DeleteJob
	ReleaseJob
)

type result struct {
	result   Result
	jobId    uint64
	priority uint32
	delay    time.Duration
}

// The function that will be performed against a unit of work.
type WorkerFunc func(*Request) Response

// A container that describes the result of consuming a unit
// of work.
type Response struct {
	Result Result      `json:"-"`
	Data   interface{} `json:"data"`
	Error  string      `json:"error"`
	Delay  int         `json:"-"`
}

// A unit of work that is passed to a WorkerFunc
type Request struct {
	id       uint64          `json:"-"`
	host     string          `json:"-"`
	Data     json.RawMessage `json:"data"`
	Feedback bool            `json:"feedback"`
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

	beanConn, dialErr := beanstalk.Dial("tcp", r.host)
	if dialErr != nil {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err.Error(),
			Delay:  delay(1),
		}
	}
	defer beanConn.Close()

	stats, statsErr := beanConn.StatsJob(r.id)
	if statsErr != nil {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err.Error(),
			Delay:  delay(1),
		}
	}

	_, ok := stats["releases"]
	if !ok {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err.Error(),
			Delay:  delay(1),
		}
	}

	releases, strErr := strconv.Atoi(stats["releases"])
	if strErr != nil {
		// send it back as retry = 1
		return Response{
			Result: ReleaseJob,
			Error:  err.Error(),
			Delay:  delay(1),
		}
	}

	if releases >= maxRetries {
		return r.BuryJob(err)
	}

	return Response{
		Result: ReleaseJob,
		Error:  err.Error(),
		Delay:  delay(releases),
	}
}

// Helper for burying a job.
func (r *Request) BuryJob(err error) Response {
	return Response{
		Result: BuryJob,
		Error:  err.Error(),
	}
}

// Helper for deleting a job.
func (r *Request) DeleteJob(err error) Response {
	return Response{
		Result: DeleteJob,
		Error:  err.Error(),
	}
}

// helper for getting response tubes
func getResponseTube(workerTube string, jobId uint64) string {
	return fmt.Sprintf("%s_%d", workerTube, jobId)
}
