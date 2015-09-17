# Go - Beanstalkd worker Test orchestration

## About

This orchestation of docker containers has

* Golang 1.5
* Beanstalkd
* Ginkgo / Gomega

Your current `github.com/eramus/worker` code will be mounted into the containers everytime you run the tests.

## Running your tests

Step into this repository's root directory (`github.com/eramus/worker`) and execute

	make test

The orchestrator file will pull, build the necesary images (if they are not already built), start the containers and run the tests.

## I just to peek into this container

Easy. Go into `test.sh` (in this very directory), uncomment the `bash` line and comment everything else.
