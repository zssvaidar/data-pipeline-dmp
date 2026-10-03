EVENTS_URL ?= http://127.0.0.1:3000/events

.PHONY: up down build invoke api simulate test deploy delete logs

up:            ## start local Redpanda (+ console on :8080)
	docker compose up -d

down:
	docker compose down

build:
	sam build

invoke: build  ## run the function once with a sample event
	sam local invoke SiteEventsFunction -e events/page_view.json

api: build     ## local HTTP API on http://127.0.0.1:3000/events
	sam local start-api

simulate:      ## send simulated site traffic (EVENTS_URL to override)
	python3 simulator/simulate.py --sessions 0 --url $(EVENTS_URL)

test:
	python3 -m pytest -q tests

deploy: build  ## first time: sam deploy --guided
	sam deploy

delete:
	sam delete

logs:
	sam logs --stack-name site-events-dev --tail
