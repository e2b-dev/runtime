ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

# Name of the clickhouse container - it's available via docker bridge network
CLICKHOUSE_HOST ?= clickhouse

.PHONY: run
run:
	# Fill in the variables in the config file
	@clickhouse_password="$(CLICKHOUSE_PASSWORD)" \
	clickhouse_host="$(CLICKHOUSE_HOST)" \
	clickhouse_username=$(CLICKHOUSE_USERNAME) \
	clickhouse_port=$(CLICKHOUSE_PORT) \
	clickhouse_database=$(CLICKHOUSE_DATABASE) \
	envsubst < ./tests/otel-collector.yaml > "./otel-collector.local.yaml"

	docker network create e2b || true
	docker run --rm --name otel-collector \
		--network e2b \
		-p 4317:4317 \
		-p 13133:13133 \
		-v ./otel-collector.local.yaml:/etc/otel-collector-config.yaml \
		otel/opentelemetry-collector-contrib:0.163.2 \
		--config /etc/otel-collector-config.yaml
