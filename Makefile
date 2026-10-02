.PHONY: up down clean logs orders etl segment pipeline test test-go test-py

up:            ## Start the always-on services (API, lander, Kafka, Postgres, MinIO)
	docker compose up -d --build --wait

down:          ## Stop everything, keep data
	docker compose --profile jobs down

clean:         ## Stop everything and delete all data volumes
	docker compose --profile jobs down -v

logs:
	docker compose logs -f api lander

orders:        ## Send sample orders: make orders N=500
	./scripts/generate_orders.sh $(or $(N),200)

etl:           ## Run the bookmarked ETL job once (Glue job run)
	docker compose run --rm --build etl

segment:       ## Build and activate the high-value segment (Athena + activation Lambda)
	docker compose run --rm --build segment

pipeline: etl segment  ## Nightly batch: ETL then segment

test: test-go test-py

test-go:
	cd services && go vet ./... && go test ./...

test-py:       ## Needs: pip install -r requirements-dev.txt (and Java 17+ for PySpark)
	python -m pytest -q etl/tests query/tests
