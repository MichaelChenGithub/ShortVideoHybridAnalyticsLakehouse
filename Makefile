.PHONY: reset-infra seed-bronze up down clean help

SCRIPTS := src/scripts

## reset-infra: Tear down, clean state, and rebuild the core pipeline stack
reset-infra:
	bash $(SCRIPTS)/reset_infra.sh

## seed-bronze: Start streaming jobs, run generator, drain, and verify MinIO
seed-bronze:
	bash $(SCRIPTS)/seed_bronze.sh

## up: Full reset + seed in one shot
up: reset-infra seed-bronze

## down: Stop all containers and remove named volumes
down:
	docker compose down -v --remove-orphans

## clean: reset-infra + wipe ivy_cache (forces jar re-download; use when deps are corrupted)
clean:
	WIPE_IVY_CACHE=1 bash $(SCRIPTS)/reset_infra.sh

## help: List available targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  make /'
