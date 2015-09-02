package worker_test

import (
	"os"

	"github.com/eramus/worker"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("New", func() {
	var (
		err          error
		myWorker     worker.Worker
		myWorkerFunc func(req *worker.Request) (res worker.Response)
		options      *worker.Options
	)

	Context("No beanstalkd connection", func() {
		BeforeEach(func() {
			options = &worker.Options{
				Host: "0.0.0.0:42000",
			}

			myWorker, err = worker.New("my-worker", myWorkerFunc, options)
		})

		It("should fail", func() {
			Expect(myWorker).Should(BeNil())
			Expect(err.Error()).Should(Equal("unable to connect to beanstalk"))
		})
	})

	Context("Beanstalkd connection", func() {
		BeforeEach(func() {
			// This env var should be in the system, if not
			// we will default to local
			beanstalkHOST := os.Getenv("BEANSTALKD_PORT_11300_TCP_ADDR")
			if len(beanstalkHOST) == 0 {
				beanstalkHOST = "0.0.0.0"
			}

			options = &worker.Options{
				Host: beanstalkHOST + ":11300",
			}

			myWorker, err = worker.New("my-worker", myWorkerFunc, options)
		})

		It("should have success", func() {
			Expect(err).Should(BeNil())
		})
	})
})
