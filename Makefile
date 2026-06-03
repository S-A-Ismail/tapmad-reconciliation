# Shortcuts for the containerized stack. (Plain `docker compose ...` works too -
# see docker-compose.yml for the equivalents.)

DATE ?= 2024-01-15
N    ?= 300

.PHONY: build pipeline dbt airflow-up airflow-down logs clean

build:            ## build the pipeline + airflow images
	docker compose build

pipeline:         ## run generate -> bronze -> silver -> gold for $(DATE)
	docker compose run --rm -e BUSINESS_DATE=$(DATE) -e N=$(N) pipeline

dbt:              ## register tables + build dbt marts and tests (local spark target)
	docker compose run --rm dbt

all: pipeline dbt ## full run: pipeline then dbt marts

airflow-up:       ## start Airflow (UI http://localhost:8080, login airflow/airflow)
	docker compose --profile airflow up -d

airflow-down:     ## stop Airflow
	docker compose --profile airflow down

logs:             ## tail airflow scheduler logs
	docker compose --profile airflow logs -f airflow-scheduler

clean:            ## stop everything and wipe the lakehouse volume
	docker compose --profile airflow down -v
