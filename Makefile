.PHONY: reset-infra seed-bronze up down clean integration-test upload-aws-scripts package-spark-libs build-generator push-generator submit-all-streaming submit-rt-content-events submit-rt-video-cdc submit-rt-user-cdc run-generator-smoke run-generator-benchmark help

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

## package-spark-libs: Zip all Spark support modules for --py-files deployment on EMR Serverless
package-spark-libs:
	cd src/spark && zip /tmp/spark_libs.zip *.py

## upload-aws-scripts: Sync Spark scripts and AWS conf to S3 (run after terraform apply)
upload-aws-scripts: package-spark-libs
	$(eval BUCKET := $(shell cd terraform && terraform output -raw warehouse_bucket))
	aws s3 sync src/spark/ s3://$(BUCKET)/scripts/spark/
	aws s3 cp /tmp/spark_libs.zip s3://$(BUCKET)/scripts/spark/spark_libs.zip
	aws s3 cp spark-defaults-aws.conf s3://$(BUCKET)/config/spark-defaults-aws.conf

## build-generator: Build the generator Docker image
build-generator:
	docker build --platform linux/amd64 -f generator/Dockerfile -t benchmark-generator .

## push-generator: Tag and push generator image to ECR
push-generator: build-generator
	$(eval ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text))
	$(eval REGION := us-east-1)
	aws ecr get-login-password --region $(REGION) \
	  | docker login --username AWS --password-stdin $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com
	docker tag benchmark-generator:latest \
	  $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/benchmark-generator:latest
	docker push $(ACCOUNT_ID).dkr.ecr.$(REGION).amazonaws.com/benchmark-generator:latest

## submit-rt-content-events: Submit content events streaming job to EMR Serverless
submit-rt-content-events: _emr-vars
	sed 's|WAREHOUSE_BUCKET|$(WAREHOUSE)|g' emr-spark-config-template.json > /tmp/emr-spark-config.json
	aws emr-serverless start-job-run --region $(REGION) \
	  --application-id $(EMR_APP) --execution-role-arn $(EMR_ROLE) \
	  --mode STREAMING \
	  --name rt-content-events \
	  --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$(WAREHOUSE)/scripts/spark/rt_content_events_aggregator.py\",\"sparkSubmitParameters\":\"--py-files s3://$(WAREHOUSE)/scripts/spark/spark_libs.zip --conf spark.emr-serverless.driver.cores=1 --conf spark.emr-serverless.driver.memory=2g --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.dynamicAllocation.maxExecutors=4 --conf spark.emr-serverless.driverEnv.RT_CONTENT_EVENTS_BOOTSTRAP_SERVERS=$(MSK) --conf spark.emr-serverless.driverEnv.RT_CONTENT_EVENTS_MSK_IAM_AUTH=true --conf spark.emr-serverless.driverEnv.RT_CONTENT_EVENTS_CHECKPOINT_RAW=s3://$(CHECKPOINTS)/jobs/rt_content_events/raw/v1 --conf spark.emr-serverless.driverEnv.RT_CONTENT_EVENTS_CHECKPOINT_GOLD=s3://$(CHECKPOINTS)/jobs/rt_content_events/gold/v1 --conf spark.emr-serverless.driverEnv.RT_CONTENT_EVENTS_CHECKPOINT_INVALID=s3://$(CHECKPOINTS)/jobs/rt_content_events/invalid/v1\"}}" \
	  --configuration-overrides file:///tmp/emr-spark-config.json

## submit-rt-video-cdc: Submit video CDC streaming job to EMR Serverless
submit-rt-video-cdc: _emr-vars
	sed 's|WAREHOUSE_BUCKET|$(WAREHOUSE)|g' emr-spark-config-template.json > /tmp/emr-spark-config.json
	aws emr-serverless start-job-run --region $(REGION) \
	  --application-id $(EMR_APP) --execution-role-arn $(EMR_ROLE) \
	  --mode STREAMING \
	  --name rt-video-cdc \
	  --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$(WAREHOUSE)/scripts/spark/rt_video_cdc_upsert.py\",\"sparkSubmitParameters\":\"--py-files s3://$(WAREHOUSE)/scripts/spark/spark_libs.zip --conf spark.emr-serverless.driver.cores=1 --conf spark.emr-serverless.driver.memory=2g --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.dynamicAllocation.maxExecutors=4 --conf spark.emr-serverless.driverEnv.RT_VIDEO_CDC_BOOTSTRAP_SERVERS=$(MSK) --conf spark.emr-serverless.driverEnv.RT_VIDEO_CDC_MSK_IAM_AUTH=true --conf spark.emr-serverless.driverEnv.RT_VIDEO_CDC_CHECKPOINT_DIM_VIDEOS=s3://$(CHECKPOINTS)/jobs/rt_video_cdc/dim_videos/v1 --conf spark.emr-serverless.driverEnv.RT_VIDEO_CDC_CHECKPOINT_RAW=s3://$(CHECKPOINTS)/jobs/rt_video_cdc/raw/v1 --conf spark.emr-serverless.driverEnv.RT_VIDEO_CDC_CHECKPOINT_INVALID_CDC_VIDEOS=s3://$(CHECKPOINTS)/jobs/rt_video_cdc/invalid/v1\"}}" \
	  --configuration-overrides file:///tmp/emr-spark-config.json

## submit-rt-user-cdc: Submit user CDC streaming job to EMR Serverless
submit-rt-user-cdc: _emr-vars
	sed 's|WAREHOUSE_BUCKET|$(WAREHOUSE)|g' emr-spark-config-template.json > /tmp/emr-spark-config.json
	aws emr-serverless start-job-run --region $(REGION) \
	  --application-id $(EMR_APP) --execution-role-arn $(EMR_ROLE) \
	  --mode STREAMING \
	  --name rt-user-cdc \
	  --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$(WAREHOUSE)/scripts/spark/rt_user_cdc_raw.py\",\"sparkSubmitParameters\":\"--py-files s3://$(WAREHOUSE)/scripts/spark/spark_libs.zip --conf spark.emr-serverless.driver.cores=1 --conf spark.emr-serverless.driver.memory=2g --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.dynamicAllocation.maxExecutors=4 --conf spark.emr-serverless.driverEnv.RT_USER_CDC_BOOTSTRAP_SERVERS=$(MSK) --conf spark.emr-serverless.driverEnv.RT_USER_CDC_MSK_IAM_AUTH=true --conf spark.emr-serverless.driverEnv.RT_USER_CDC_CHECKPOINT_RAW=s3://$(CHECKPOINTS)/jobs/rt_user_cdc/raw/v1 --conf spark.emr-serverless.driverEnv.RT_USER_CDC_CHECKPOINT_INVALID=s3://$(CHECKPOINTS)/jobs/rt_user_cdc/invalid/v1\"}}" \
	  --configuration-overrides file:///tmp/emr-spark-config.json

## submit-all-streaming: Submit all 3 Spark SS jobs with stagger so each claims executors before the next starts
submit-all-streaming: _emr-vars
	$(MAKE) submit-rt-content-events
	sleep 10
	$(MAKE) submit-rt-video-cdc
	sleep 10
	$(MAKE) submit-rt-user-cdc

## run-generator-smoke: Launch 1 generator task (10 min, 500 events/sec) — verifies MSK connectivity
run-generator-smoke: _ecs-vars
	aws ecs run-task --region $(REGION) \
	  --cluster $(CLUSTER) \
	  --task-definition $(GEN_TASK_DEF) \
	  --launch-type FARGATE \
	  --network-configuration "awsvpcConfiguration={subnets=[$(SUBNET)],securityGroups=[$(SG)],assignPublicIp=DISABLED}" \
	  --overrides '{"containerOverrides":[{"name":"generator","command":["--seed","99","--duration-minutes","10","--events-per-sec","500"]}]}'

## run-generator-benchmark: Launch 4 generator tasks in parallel (seeds 1-4, 25K events/sec each = 100K total)
run-generator-benchmark: _ecs-vars
	@for seed in 1 2 3 4; do \
	  aws ecs run-task --region $(REGION) \
	    --cluster $(CLUSTER) \
	    --task-definition $(GEN_TASK_DEF) \
	    --launch-type FARGATE \
	    --network-configuration "awsvpcConfiguration={subnets=[$(SUBNET)],securityGroups=[$(SG)],assignPublicIp=DISABLED}" \
	    --overrides "{\"containerOverrides\":[{\"name\":\"generator\",\"command\":[\"--seed\",\"$$seed\"]}]}" \
	    --query 'tasks[0].taskArn' --output text; \
	done

# ── Internal: resolve ECS values for generator targets ───────────────────────
.PHONY: _ecs-vars
_ecs-vars:
	$(eval REGION      := us-east-1)
	$(eval CLUSTER     := $(shell cd terraform && terraform output -raw ecs_cluster_arn))
	$(eval GEN_TASK_DEF := $(shell cd terraform && terraform output -raw generator_task_definition))
	$(eval SUBNET      := $(shell aws ec2 describe-subnets --region us-east-1 \
	  --filters "Name=tag:Name,Values=lakehouse-private-a" \
	  --query "Subnets[0].SubnetId" --output text))
	$(eval SG          := $(shell aws ec2 describe-security-groups --region us-east-1 \
	  --filters "Name=group-name,Values=lakehouse-ecs-tasks-sg" \
	  --query "SecurityGroups[0].GroupId" --output text))

# ── Internal: resolve AWS values from terraform outputs ───────────────────────
.PHONY: _emr-vars
_emr-vars:
	$(eval REGION      := us-east-1)
	$(eval WAREHOUSE   := $(shell cd terraform && terraform output -raw warehouse_bucket))
	$(eval CHECKPOINTS := $(shell cd terraform && terraform output -raw checkpoints_bucket))
	$(eval EMR_APP     := $(shell cd terraform && terraform output -raw emr_application_id))
	$(eval EMR_ROLE    := $(shell cd terraform && terraform output -raw emr_execution_role_arn))
	$(eval MSK         := $(shell cd terraform && terraform output -raw msk_bootstrap_brokers_sasl_iam))

## help: List available targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  make /'
