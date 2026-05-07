# Cloud Workflows orchestration for the daily PV-site backfill.
#
# This workflow backfills both PV output and weather data for the full set
# of PV sites, covering a 28-day window that marches backwards through
# history with each daily run.
#
# Execution flow:
#
#   1. Invoke the Cloud Run Job with JOB_TYPE=plan_pv_site_backfill, which
#      writes a JSON manifest to GCS containing the 28-day window to process.
#   2. Read the manifest from GCS to obtain start_date and end_date.
#   3. Load the GCS checkpoint (if any) so a re-triggered run skips sites
#      that already completed in a previous execution.
#   4. Dispatch PV extraction jobs *sequentially* per PV system (PVOutput
#      rate-limits at 300 requests/hour; 10 sites × 28 days = 280 calls).
#      After each site succeeds the checkpoint is updated in GCS.
#   5. Dispatch weather extraction jobs in *parallel* per PV system (OpenMeteo
#      rate limits are generous enough for 10–20 concurrent calls).
#   6. Invoke the Cloud Run Job with JOB_TYPE=commit_pv_site_backfill, which
#      advances the cursor so tomorrow's run picks up from the next window.
#   7. Delete the checkpoint (successful run) and consolidate logs.
#
# If any extraction job fails, the workflow aborts without committing the
# cursor, so tomorrow's plan job will re-derive the same manifest and retry.
# Re-triggering today's run manually will skip already-processed sites.

resource "google_workflows_workflow" "pv_sites_backfill" {
  name                = "pv-prospect-extract-pv-sites-backfill"
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"
  description         = "Orchestrates daily PV-site backfill (PV output + weather) via manifest-driven Cloud Run Job dispatch"

  source_contents = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - project_id: $${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
              - region: "${var.region}"
              - job_name: "${var.cloud_run_job_name}"
              - workflow_name: "pv-prospect-extract-pv-sites-backfill"
              - bucket: "${var.staging_bucket_name}"
              - manifest_object: "${var.manifest_object_path}"
              - checkpoint_object: "${var.checkpoint_object_path}"
              - pv_system_ids: $${default(map.get(args, "pv_system_ids"), ${jsonencode(var.default_pv_system_ids)})}
              - pv_data_source: $${default(map.get(args, "pv_data_source"), "${var.pv_data_source}")}
              - weather_data_source: $${default(map.get(args, "weather_data_source"), "${var.weather_data_source}")}
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
                          value: plan_pv_site_backfill
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

        # Load any existing checkpoint so a re-triggered run skips already-done
        # sites.  If no checkpoint exists the try block raises a 404 and we
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
                      data: $${"Resuming from checkpoint — " + string(len(keys(completed))) + " sites already done"}
            except:
              as: checkpoint_err
              steps:
                - init_completed:
                    assign:
                      - completed: {}

        - extract_pv:
            for:
              value: pv_system_id
              in: $${pv_system_ids}
              steps:
                # Skip sites that already succeeded in a previous execution.
                - check_already_done:
                    switch:
                      - condition: $${map.get(completed, string(pv_system_id)) == true}
                        next: next_pv_site
                - log_pv_site:
                    call: sys.log
                    args:
                      data: $${"Extracting PV system " + string(pv_system_id)}
                - run_pv_extract:
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
                                  value: $${pv_data_source}
                                - name: PV_SYSTEM_ID
                                  value: $${string(pv_system_id)}
                                - name: START_DATE
                                  value: $${manifest.start_date}
                                - name: END_DATE
                                  value: $${manifest.end_date}
                                - name: DRY_RUN
                                  value: $${dry_run}
                                - name: SPLIT_BY
                                  value: day
                                - name: WORKFLOW_NAME
                                  value: $${workflow_name}
                    result: pv_op
                - wait_for_pv_op:
                    call: wait_for_operation
                    args:
                      operation_name: $${pv_op.name}
                    result: pv_op_done
                - wait_for_pv:
                    call: await_job
                    args:
                      execution_name: $${pv_op_done.response.name}
                # Mark this site done and persist the checkpoint so a re-run
                # can skip it without re-extracting.
                - mark_done:
                    assign:
                      - completed[string(pv_system_id)]: true
                - write_checkpoint:
                    call: http.post
                    args:
                      url: $${"https://storage.googleapis.com/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=" + text.url_encode(checkpoint_object)}
                      auth:
                        type: OAuth2
                      headers:
                        Content-Type: application/json
                      body: $${json.encode_to_string(completed)}
                - next_pv_site:
                    assign:
                      - _: null

        - extract_weather:
            steps:
              - log_weather_start:
                  call: sys.log
                  args:
                    data: $${"Starting parallel weather extraction"}
              - extract_weather_parallel:
                  parallel:
                    for:
                      value: pv_system_id
                      in: $${pv_system_ids}
                      steps:
                        - run_weather_extract:
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
                                          value: weather
                                        - name: PV_SYSTEM_ID
                                          value: $${string(pv_system_id)}
                                        - name: START_DATE
                                          value: $${manifest.start_date}
                                        - name: END_DATE
                                          value: $${manifest.end_date}
                                        - name: WORKFLOW_NAME
                                          value: $${workflow_name}
                            result: weather_op
                        - wait_for_weather_op:
                            call: wait_for_operation
                            args:
                              operation_name: $${weather_op.name}
                            result: weather_op_done
                        - wait_for_weather:
                            call: await_job
                            args:
                              execution_name: $${weather_op_done.response.name}

        - commit:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: JOB_TYPE
                          value: commit_pv_site_backfill
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
        # written because all sites were already skipped).
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
