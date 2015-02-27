package worker

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

var (
	ErrBeanstalkConnect = errors.New("unable to connect to beanstalk")
	ErrJSONMarshal      = errors.New("json marshal")
	ErrUnableToSend     = errors.New("unable to send json")
	ErrNoResponse       = errors.New("did not receive a response")
	ErrNoConn           = errors.New("no worker conn")
	ErrUnknownResult    = errors.New("unknown result of response")
	ErrBadJob           = errors.New("got a bad job")
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
	Wait     time.Duration
}

var defaultOptions = &Options{
	Host:     "0.0.0.0:11300",
	Count:    1,
	Reserve:  (2 * time.Second),
	Priority: 0,
	Delay:    time.Duration(0),
	TTR:      (3600 * time.Second),
	Wait:     (10 * time.Second),
}

// Get a copy of the default options.
func GetDefaults() Options {
	return *defaultOptions
}

// A function for determining the amount of delay that should be
// used each time a job is released.
type DelayDecay func(int) time.Duration

var defaultDecay = func(retries int) time.Duration {
	return time.Second
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
	jobID    uint64
	priority uint32
	delay    time.Duration
}

// The function that will be performed against a unit of work.
type Func func(*Request) Response

type Request interface {
	RetryJob(error, int, DelayDecay)
	BuryJob(error) Response
	DeleteJob(error) Response
	Handle(*Response) error
	Finish(Response) error
}

// A unit of work that is passed to a WorkerFunc
type DefaultRequest struct {
	Data     json.RawMessage `json:"data"`
	Feedback bool            `json:"feedback"`
}

type beanstalkRequest struct {
	DefaultRequest
	tube string
	id       uint64          `json:"-"`
	options *Options
//	host     string          `json:"-"`
}

// Helper function for retrying a job. This accepts an error and a
// number of times that a unit of work should be retried. An optional
// DelayDecay func is accepted for setting the amount of time, based
// on number of releases, that a unit of work should be delayed before
// acting against it again.
func (r *beanstalkConn) RetryJob(err error, maxRetries int, delay DelayDecay) Response {
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
func (r *beanstalkConn) BuryJob(err error) Response {
	return Response{
		Result: BuryJob,
		Error:  err.Error(),
	}
}

// Helper for deleting a job.
func (r *beanstalkConn) DeleteJob(err error) Response {
	return Response{
		Result: DeleteJob,
		Error:  err.Error(),
	}
}
