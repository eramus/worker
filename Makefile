test:
	@docker version >/dev/null || { echo >&2 "Docker is needed. Aborting."; exit 1; }
	@docker-compose -v >/dev/null || { echo >&2 "Docker Compose is needed. Aborting."; exit 1; }

	@docker-compose -f ./docker/test.yml kill >/dev/null
	@docker-compose -f ./docker/test.yml rm -fv >/dev/null

	@docker-compose -f ./docker/test.yml run --rm eramus_worker_test || echo

	@docker-compose -f ./docker/test.yml kill >/dev/null
	@docker-compose -f ./docker/test.yml rm -fv >/dev/null
