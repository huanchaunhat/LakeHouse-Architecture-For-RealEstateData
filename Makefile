COMPOSE := docker compose --env-file .env.local -f docker-compose.local.yml

.PHONY: init config build up down status logs reset

init:
	@test -f .env.local || cp .env.example .env.local
	@mkdir -p airflow/logs airflow/plugins
	@echo "Edit .env.local, then run: make up"

config:
	$(COMPOSE) config --quiet

build:
	$(COMPOSE) build

up: config
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

reset:
	@echo "Run manually if you really want to delete all local data:"
	@echo "$(COMPOSE) down --volumes --remove-orphans"
