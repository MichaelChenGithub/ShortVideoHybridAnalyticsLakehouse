.PHONY: reset-infra seed-bronze up down clean integration-test help

SCRIPTS := src/scripts

## reset-infra: Tear down, clean state, and rebuild the core pipeline stack
reset-infra:
	bash $(SCRIPTS)/reset_infra.sh

## seed-bronze: Start streaming jobs, run generator, drain, and verify MinIO
seed-bronze:
	bash $(SCRIPTS)/seed_bronze.sh

## up: Full reset + seed in one shot
up: reset-infra seed-bronze

## integration-test: Full reset + seed then run all 6 acceptance scripts
integration-test: up
	bash $(SCRIPTS)/run_realtime_signoff_acceptance.sh
	bash $(SCRIPTS)/run_bt_dim_users_scd2_acceptance.sh
	bash $(SCRIPTS)/run_bt_dim_videos_scd2_acceptance.sh
	bash $(SCRIPTS)/run_bt_events_conformed_acceptance.sh
	bash $(SCRIPTS)/run_bt_user_activity_sessions_30m_acceptance.sh
	bash $(SCRIPTS)/run_rule_baseline_publish_acceptance.sh

## down: Stop all containers and remove named volumes
down:
	docker compose down -v --remove-orphans

## clean: reset-infra + wipe ivy_cache (forces jar re-download; use when deps are corrupted)
clean:
	WIPE_IVY_CACHE=1 bash $(SCRIPTS)/reset_infra.sh

## help: List available targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  make /'
