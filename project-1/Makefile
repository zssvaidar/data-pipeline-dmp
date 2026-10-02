.PHONY: up down clean logs orders etl segment pipeline test test-go test-py lint-aws aws-deploy aws-orders aws-pipeline aws-destroy

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

# ---------- AWS (phase 2) ----------
# Needs AWS credentials, the AWS CLI and the SAM CLI. STACK_NAME defaults to dmp.

lint-aws:      ## Validate the CloudFormation/SAM template offline
	cfn-lint infra/template.yaml

aws-deploy:    ## Build + deploy the stack and upload the Glue scripts
	./infra/deploy.sh

aws-orders:    ## Send sample orders to the deployed API: make aws-orders N=500
	API_URL="$$(aws cloudformation describe-stacks --stack-name $${STACK_NAME:-dmp} \
	  --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text)" \
	  ./scripts/generate_orders.sh $(or $(N),200)

aws-pipeline:  ## Run crawler -> ETL -> activation now and print the segment
	./infra/run_pipeline.sh

aws-destroy:   ## Delete the stack and all its data (asks first)
	./infra/destroy.sh
