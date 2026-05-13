# Cloud Workflows orchestration for the daily weather grid backfill.
#
# This workflow is manifest-driven:
#
#   1. It invokes the Cloud Run Job with JOB_TYPE=plan_weather_grid_backfill,
#      which writes a JSON manifest to GCS describing the day's work. Each
#      batch's env carries the resolved LOCATIONS list for one
#      (sample_file × window) pair; per-site ledger entries are recorded by
#      the container as it iterates.
#   2. It reads the manifest from GCS to obtain the ordered list of batches.
#   3. It loads a GCS position checkpoint {"next_batch_index": N} (if any) so
#      that a re-triggered run resumes where the previous run stopped.
#   4. It dispatches one extract_and_load Cloud Run Job per batch, sleeping
#      between dispatches to stay under OpenMeteo's per-hour rate limit.
#      The checkpoint is advanced unconditionally after each dispatch
#      (success or exit-1 swallowed crash), so the workflow always marches
#      forward at a predictable cadence rather than retrying failures.
#      NOTE: To fully respect the 5k/hr limit, the workflow accepts a
#      `max_batches_per_run` parameter and is triggered twice by Cloud Scheduler.
#      Run 1 processes the first few batches and exits early; Run 2 resumes
#      from the checkpoint to finish the remaining batches.
#   5. After all batches in the manifest are attempted, it invokes the Cloud
#      Run Job with JOB_TYPE=commit_weather_grid_backfill, which advances
#      the live cursor so that tomorrow's run picks up from the next point
#      in the backfill.
#   6. Deletes the checkpoint (full run) and consolidates logs. Partial runs
#      skip commit + checkpoint deletion but still consolidate logs so the
#      ledger is always finalised.

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
              # Workflow trigger date (UTC), pinned once and propagated to every
              # task as RUN_DATE. Per-batch START_DATEs (each batch's data
              # window) are separately set from the manifest.
              - run_date: $${text.substring(time.format(sys.now()), 0, 10)}
              - manifest_object: $${"tracking/manifests/" + run_date + "/" + workflow_name + ".backfill.json"}
              - checkpoint_object: "${var.checkpoint_object_path}"
              - data_source: $${default(map.get(args, "data_source"), "${var.data_source}")}
              - sleep_seconds: $${default(map.get(args, "sleep_seconds_between_batches"), ${var.sleep_seconds_between_batches})}
              - max_batches_per_run: $${default(map.get(args, "max_batches_per_run"), 100)}
              - run_count: 0
              - dry_run: $${default(map.get(args, "dry_run"), "false")}
              - next_batch_index: 0

        - run_pipeline:
            try:
              steps:
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
                                - name: RUN_DATE
                                  value: $${run_date}
                                - name: DATA_SOURCE
                                  value: $${data_source}
                                - name: DRY_RUN
                                  value: $${dry_run}
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

                # The plan job emits the orchestrator phases-shape: a single
                # phase containing one TASK_HASH-injected env list per batch.
                # We dispatch each task verbatim — no inline env construction.
                - collect_batches:
                    assign:
                      - batches: $${manifest.phases[0]}

                # Load the position checkpoint so a re-triggered run resumes
                # from where the previous run stopped. The object stores a
                # single integer {"next_batch_index": N}. 404 (no prior run)
                # is treated as N=0.
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
                              - next_batch_index: $${default(map.get(json.decode(checkpoint_raw), "next_batch_index"), 0)}
                        - log_resuming:
                            call: sys.log
                            args:
                              data: $${"Resuming from checkpoint — starting at batch " + string(next_batch_index)}
                    except:
                      as: checkpoint_err
                      steps:
                        - init_next_index:
                            assign:
                              - next_batch_index: 0

                - dispatch_init:
                    assign:
                      - i: $${next_batch_index}

                - dispatch_loop:
                    switch:
                      - condition: $${i >= len(batches)}
                        next: check_all_done

                - get_batch:
                    assign:
                      - batch: $${batches[i]}

                - check_run_limit:
                    switch:
                      - condition: $${run_count >= max_batches_per_run}
                        next: check_all_done

                - increment_run_count:
                    assign:
                      - run_count: $${run_count + 1}

                - maybe_sleep:
                    switch:
                      - condition: $${run_count > 1}
                        steps:
                          - pace:
                              call: sys.sleep
                              args:
                                seconds: $${sleep_seconds}

                - log_batch_start:
                    call: sys.log
                    args:
                      data: $${"Starting weather extraction for batch " + string(i+1) + "/" + string(len(batches))}

                # With the container's per-site idempotency and exit-0-on-
                # site-failure policy, the workflow no longer retries at
                # this layer. Exit 1 (container crash) is logged and the
                # checkpoint still advances — the workflow marches forward
                # rather than retrying. Exit 2 (WorkflowTerminatingError)
                # re-raises out of run_pipeline so the cursor commit is
                # skipped.
                - run_batch:
                    try:
                      steps:
                        - start_job:
                            call: googleapis.run.v2.projects.locations.jobs.run
                            args:
                              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                              body:
                                overrides:
                                  containerOverrides:
                                    - env: $${batch}
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
                    except:
                      as: task_err
                      steps:
                        - extract_exit_code:
                            assign:
                              - failed_exit_code: $${default(map.get(task_err, ["data", "exit_code"]), 1)}
                        - branch_on_exit_code:
                            switch:
                              - condition: $${failed_exit_code == 2}
                                raise: $${task_err}
                        - log_task_failure:
                            call: sys.log
                            args:
                              data: '$${"Batch crashed (exit_code=" + string(failed_exit_code) + "), advancing anyway: " + json.encode_to_string(task_err)}'
                              severity: "WARNING"

                # Advance the position checkpoint unconditionally after
                # every dispatched batch. The container records per-site
                # outcomes in the ledger; the workflow's role here is just
                # to track "how far through the manifest are we?" so a
                # same-day re-trigger picks up cleanly.
                - advance_checkpoint:
                    call: http.post
                    args:
                      url: $${"https://storage.googleapis.com/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=" + text.url_encode(checkpoint_object)}
                      auth:
                        type: OAuth2
                      headers:
                        Content-Type: application/json
                      body: '$${"{\"next_batch_index\": " + string(i + 1) + "}"}'

                - next_batch:
                    assign:
                      - i: $${i + 1}
                    next: dispatch_loop

                # Commit once every batch in the manifest has been attempted
                # (success or exit-1 swallowed crash). Exit-2 failures never
                # reach here — they re-raise out of run_batch and abort
                # run_pipeline, skipping the commit. Throttled-early runs
                # (max_batches_per_run hit) jump past commit/delete to
                # pipeline_complete so the cursor stays put for the next
                # trigger; either path then falls through to consolidate_logs
                # so the ledger is always finalised.
                - check_all_done:
                    switch:
                      - condition: $${i == len(batches)}
                        next: commit
                      - condition: true
                        next: pipeline_complete

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
                                - name: RUN_DATE
                                  value: $${run_date}
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

                # Sentinel for partial runs to land on without committing
                # the cursor or deleting the checkpoint. Falls through to
                # the end of run_pipeline.try so consolidate_logs runs.
                - pipeline_complete:
                    assign:
                      - _: null
            except:
              as: workflow_err
              steps:
                - log_workflow_error:
                    call: sys.log
                    args:
                      data: '$${"Workflow encountered an error - proceeding to log consolidation. Error: " + json.encode_to_string(workflow_err)}'
                      severity: "ERROR"


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
                        - name: RUN_DATE
                          value: $${run_date}
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

    retry_predicate:
      params: [e]
      steps:
        # Use map.get + default so non-HTTP errors (e.g. TimeoutError, OSError)
        # without a "code" field don't blow up the predicate with a KeyError.
        - extract_error_code:
            assign:
              - error_code: $${default(map.get(e, "code"), 0)}
        - check_retriable:
            switch:
              # Retry on 429 (Too Many Requests), 500, 503, or our own failure message
              - condition: $${error_code == 429 or error_code == 500 or error_code == 503}
                return: true
              - condition: $${text.match_regex(default(e.message, ""), "Job execution failed")}
                return: true
              - condition: true
                return: false


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
                steps:
                  - read_exit_code:
                      call: fetch_failed_task_exit_code
                      args:
                        execution_name: $${execution_name}
                      result: exit_code
                  - raise_failure:
                      raise:
                        message: '$${"Job execution failed or was cancelled (succeeded: " + string(default(exec.succeededCount, 0)) + "/" + string(exec.taskCount) + ", exit_code: " + string(exit_code) + ")"}'
                        data:
                          exec: $${exec}
                          exit_code: $${exit_code}

    # Reads the failed task's container exit code. Defaults to 1 if the
    # task list is empty or the exitCode field is absent (e.g. the
    # container never ran). The exit code drives the per-batch except's
    # decision to swallow (1: task failed, workflow continues) or
    # re-raise (2: WorkflowTerminatingError -- workflow aborts before the
    # cursor-commit step, leaving the cursor unchanged for tomorrow).
    fetch_failed_task_exit_code:
      params: [execution_name]
      steps:
        - list_tasks:
            call: googleapis.run.v2.projects.locations.jobs.executions.tasks.list
            args:
              parent: $${execution_name}
            result: tasks_response
        - check_tasks_present:
            switch:
              - condition: $${len(default(map.get(tasks_response, "tasks"), [])) == 0}
                return: 1
        - extract_first_task_exit_code:
            assign:
              - first_task: $${tasks_response.tasks[0]}
              - exit_code: $${default(map.get(first_task, ["status", "lastAttemptResult", "exitCode"]), 1)}
        - return_exit_code:
            return: $${exit_code}
  YAML
}
