# Cloud Workflows orchestration for the daily weather grid backfill.
#
# This workflow is manifest-driven:
#
#   1. It invokes the Cloud Run Job with JOB_TYPE=plan_weather_grid_backfill,
#      which writes a JSON manifest to GCS describing the day's work.
#   2. It reads the manifest from GCS to obtain the ordered list of batches.
#   3. It loads a GCS checkpoint (if any) so that a re-triggered run skips
#      batches that already completed in a previous execution.
#   4. It dispatches one extract_and_load Cloud Run Job per batch, sleeping
#      between dispatches to stay under OpenMeteo's per-hour rate limit.
#      After each batch succeeds the checkpoint is updated in GCS.
#   5. After all batches succeed, it invokes the Cloud Run Job with
#      JOB_TYPE=commit_weather_grid_backfill, which advances the live cursor
#      so that tomorrow's run picks up from the next point in the backfill.
#   6. Deletes the checkpoint (successful run) and consolidates logs.
#
# If any batch fails, the workflow aborts without committing the cursor, so
# tomorrow's plan job will re-derive the same manifest and retry.
# Re-triggering today's run manually will skip already-completed batches.

resource "google_workflows_workflow" "weather_grid_backfill" {
  name                = "pv-prospect-extract-weather-grid-backfill"
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"
  description         = "Orchestrates the daily grid-point weather backfill via a manifest + paced Cloud Run Job dispatch"

  source_contents = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - project_id: $${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
              - region: "${var.region}"
              - job_name: "${var.cloud_run_job_name}"
              - workflow_name: "pv-prospect-extract-weather-grid-backfill"
              - bucket: "${var.staging_bucket_name}"
              - manifest_object: "${var.manifest_object_path}"
              - checkpoint_object: "${var.checkpoint_object_path}"
              - data_source: $${default(map.get(args, "data_source"), "${var.data_source}")}
              - sleep_seconds: $${default(map.get(args, "sleep_seconds_between_batches"), ${var.sleep_seconds_between_batches})}
              - dry_run: $${default(map.get(args, "dry_run"), "false")}
              - completed: {}

        - plan:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: JOB_TYPE
                          value: plan_weather_grid_backfill
                        - name: WORKFLOW_NAME
                          value: $${workflow_name}
            result: plan_op

        - wait_for_plan_op:
            call: wait_for_operation
            args:
              operation_name: $${plan_op.name}
            result: plan_op_done

        - wait_for_plan:
            call: await_job
            args:
              execution_name: $${plan_op_done.response.name}

        - fetch_manifest:
            call: googleapis.storage.v1.objects.get
            args:
              bucket: $${bucket}
              object: $${text.url_encode(manifest_object)}
              alt: media
            result: manifest_raw

        - decode_manifest:
            assign:
              - manifest: $${json.decode(manifest_raw)}

        - collect_batches:
            assign:
              - batches: $${list.concat([manifest.step2_batch], manifest.step3_batches)}

        # Load any existing checkpoint so a re-triggered run skips already-done
        # batches.  If no checkpoint exists the try block raises a 404 and we
        # fall through to init_completed, which leaves `completed` as {}.
        - load_checkpoint:
            try:
              steps:
                - fetch_checkpoint:
                    call: googleapis.storage.v1.objects.get
                    args:
                      bucket: $${bucket}
                      object: $${text.url_encode(checkpoint_object)}
                      alt: media
                    result: checkpoint_raw
                - decode_checkpoint:
                    assign:
                      - completed: $${json.decode(checkpoint_raw)}
                - log_resuming:
                    call: sys.log
                    args:
                      data: $${"Resuming from checkpoint — " + string(len(keys(completed))) + " batches already done"}
            except:
              as: checkpoint_err
              steps:
                - init_completed:
                    assign:
                      - completed: {}

        - dispatch:
            for:
              value: batch
              index: i
              in: $${batches}
              steps:
                # Skip batches that already succeeded in a previous execution.
                - check_already_done:
                    switch:
                      - condition: $${map.get(completed, string(i)) == true}
                        next: next_batch
                - maybe_sleep:
                    switch:
                      - condition: $${i > 0}
                        steps:
                          - pace:
                              call: sys.sleep
                              args:
                                seconds: $${sleep_seconds}
                - log_batch_start:
                    call: sys.log
                    args:
                      data: $${"Starting weather extraction for batch " + string(i+1) + "/" + string(len(batches)) + " (sample_file_index=" + string(batch.sample_file_index) + ")"}
                - run_batch:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                      body:
                        overrides:
                          containerOverrides:
                            - env:
                                - name: JOB_TYPE
                                  value: extract_and_load
                                - name: DATA_SOURCE
                                  value: $${data_source}
                                - name: SAMPLE_FILE_INDEX
                                  value: $${string(batch.sample_file_index)}
                                - name: START_DATE
                                  value: $${batch.start_date}
                                - name: END_DATE
                                  value: $${batch.end_date}
                                - name: DRY_RUN
                                  value: $${dry_run}
                                - name: WORKFLOW_NAME
                                  value: $${workflow_name}
                    result: batch_op
                - wait_for_batch_op:
                    call: wait_for_operation
                    args:
                      operation_name: $${batch_op.name}
                    result: batch_op_done
                - wait_for_batch:
                    call: await_job
                    args:
                      execution_name: $${batch_op_done.response.name}
                # Mark this batch done and persist the checkpoint so a re-run
                # can skip it without re-extracting.
                - mark_done:
                    assign:
                      - completed[string(i)]: true
                - write_checkpoint:
                    call: http.post
                    args:
                      url: $${"https://storage.googleapis.com/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=" + text.url_encode(checkpoint_object)}
                      auth:
                        type: OAuth2
                      headers:
                        Content-Type: application/json
                      body: $${json.encode_to_string(completed)}
                - next_batch:
                    assign:
                      - _: null

        - commit:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: JOB_TYPE
                          value: commit_weather_grid_backfill
                        - name: WORKFLOW_NAME
                          value: $${workflow_name}
            result: commit_op

        - wait_for_commit_op:
            call: wait_for_operation
            args:
              operation_name: $${commit_op.name}
            result: commit_op_done

        - wait_for_commit:
            call: await_job
            args:
              execution_name: $${commit_op_done.response.name}

        # Delete the checkpoint on success so tomorrow's scheduled run starts
        # fresh.  Silently ignore errors (e.g. 404 if the checkpoint was never
        # written because all batches were already skipped).
        - delete_checkpoint:
            try:
              call: googleapis.storage.v1.objects.delete
              args:
                bucket: $${bucket}
                object: $${text.url_encode(checkpoint_object)}
            except:
              as: delete_err
              steps:
                - ignore_delete_error:
                    assign:
                      - _: null

        - consolidate_logs:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: JOB_TYPE
                          value: consolidate_logs
                        - name: WORKFLOW_NAME
                          value: $${workflow_name}
            result: consolidate_logs_op

        - wait_for_consolidate_op:
            call: wait_for_operation
            args:
              operation_name: $${consolidate_logs_op.name}
            result: consolidate_logs_op_done

        - wait_for_consolidate:
            call: await_job
            args:
              execution_name: $${consolidate_logs_op_done.response.name}

        - done:
            return: "completed"

    wait_for_operation:
      params: [operation_name]
      steps:
        - check_op:
            call: googleapis.run.v2.projects.locations.operations.get
            args:
              name: $${operation_name}
            result: op
        - evaluate_op:
            switch:
              - condition: $${default(op.done, false)}
                return: $${op}
              - condition: true
                steps:
                  - wait:
                      call: sys.sleep
                      args:
                        seconds: 2
                  - retry_op:
                      next: check_op

    await_job:
      params: [execution_name]
      steps:
        - check_status:
            call: googleapis.run.v2.projects.locations.jobs.executions.get
            args:
              name: $${execution_name}
            result: exec
        - evaluate_status:
            switch:
              - condition: $${map.get(exec, "completionTime") == null}
                steps:
                  - wait:
                      call: sys.sleep
                      args:
                        seconds: 10
                  - retry:
                      next: check_status
              - condition: $${exec.succeededCount == exec.taskCount}
                return: $${exec}
              - condition: true
                raise:
                  message: '$${"Job execution failed or was cancelled (succeeded: " + string(default(exec.succeededCount, 0)) + "/" + string(exec.taskCount) + ")"}'
                  data: $${exec}
  YAML
}
