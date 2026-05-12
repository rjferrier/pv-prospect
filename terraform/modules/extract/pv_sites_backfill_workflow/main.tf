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
# Failures branch on the container exit code surfaced by await_job:
#   exit 1 (general task failure)        -- log, swallow, continue the
#       loop / parallel branch. The cursor commit at the end still runs,
#       so tomorrow's run advances. Failed PV tasks are *not* recorded in
#       the legacy checkpoint, so a same-day re-trigger will re-attempt
#       them.
#   exit 2 (WorkflowTerminatingError)    -- re-raise. The outer
#       run_pipeline try aborts before the commit step, leaving the
#       cursor unchanged for tomorrow. consolidate_logs still runs.
# Re-triggering today's run manually will skip already-completed sites
# (those persisted in the checkpoint).

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
              # Workflow trigger date (UTC), pinned once and propagated to every
              # task as RUN_DATE. Distinct from manifest.start_date / end_date
              # (the data window the backfill is processing).
              - run_date: $${text.substring(time.format(sys.now()), 0, 10)}
              - manifest_object: $${"tracking/manifests/" + run_date + "/" + workflow_name + ".backfill.json"}
              - checkpoint_object: "${var.checkpoint_object_path}"
              - pv_system_ids: $${default(map.get(args, "pv_system_ids"), ${jsonencode(var.default_pv_system_ids)})}
              - pv_data_source: $${default(map.get(args, "pv_data_source"), "${var.pv_data_source}")}
              - weather_data_source: $${default(map.get(args, "weather_data_source"), "${var.weather_data_source}")}
              - dry_run: $${default(map.get(args, "dry_run"), "false")}
              - completed: {}

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
                                  value: plan_pv_site_backfill
                                - name: WORKFLOW_NAME
                                  value: $${workflow_name}
                                - name: RUN_DATE
                                  value: $${run_date}
                                - name: PV_SYSTEM_IDS
                                  value: $${json.encode_to_string(pv_system_ids)}
                                - name: PV_DATA_SOURCE
                                  value: $${pv_data_source}
                                - name: WEATHER_DATA_SOURCE
                                  value: $${weather_data_source}
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

                # The plan job emits the orchestrator phases-shape alongside the
                # date window and next cursor. phases[0] is the sequential PV
                # task list (one TASK_HASH-injected env per system), phases[1]
                # is the parallel weather task list.
                - decode_manifest:
                    assign:
                      - manifest: $${json.decode(manifest_raw)}
                      - pv_tasks: $${manifest.phases[0]}
                      - weather_tasks: $${manifest.phases[1]}

                # Load any existing checkpoint so a re-triggered run skips already-done
                # PV tasks. The checkpoint key is the per-task index into phases[0]
                # (PV tasks only — weather tasks are dispatched in parallel and not
                # individually checkpointed). If no checkpoint exists the try block
                # raises a 404 and we fall through to init_completed, which leaves
                # `completed` as {}.
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
                              data: $${"Resuming from checkpoint — " + string(len(keys(completed))) + " PV tasks already done"}
                    except:
                      as: checkpoint_err
                      steps:
                        - init_completed:
                            assign:
                              - completed: {}

                - extract_pv_init:
                    assign:
                      - i: 0
                - extract_pv_loop:
                    switch:
                      - condition: $${i >= len(pv_tasks)}
                        next: extract_weather
                - get_pv_task:
                    assign:
                      - pv_task: $${pv_tasks[i]}
                - check_pv_already_done:
                    switch:
                      - condition: $${map.get(completed, string(i)) == true}
                        next: next_pv_task
                - log_pv_start:
                    call: sys.log
                    args:
                      data: $${"Extracting PV task " + string(i+1) + "/" + string(len(pv_tasks))}

                # Track whether this task ultimately succeeded so the legacy
                # checkpoint is only updated when the run didn't fail-and-swallow.
                - init_pv_succeeded:
                    assign:
                      - pv_task_succeeded: true

                - run_pv_task:
                    try:
                      steps:
                        - run_pv_extract:
                            call: googleapis.run.v2.projects.locations.jobs.run
                            args:
                              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                              body:
                                overrides:
                                  containerOverrides:
                                    - env: $${pv_task}
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
                    retry:
                      predicate: $${retry_predicate}
                      max_retries: 3
                      backoff:
                        initial_delay: 60
                        max_delay: 600
                        multiplier: 2
                    # After retries are exhausted, branch on the container exit
                    # code surfaced by await_job: 2 -> re-raise so the workflow
                    # aborts before the commit step; 1 (or unknown) -> log, mark
                    # this PV task as not succeeded (so the legacy checkpoint
                    # isn't updated), and let the loop carry on to the next task.
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
                        - mark_pv_task_failed:
                            assign:
                              - pv_task_succeeded: false
                        - log_task_failure:
                            call: sys.log
                            args:
                              data: '$${"PV task failed (exit_code=" + string(failed_exit_code) + "), continuing: " + json.encode_to_string(task_err)}'
                              severity: "WARNING"

                # Mark this task done and persist the checkpoint so a re-run
                # can skip it without re-extracting -- only on success, so
                # a swallowed exit-1 failure stays retriable on re-trigger.
                - maybe_record_pv_done:
                    switch:
                      - condition: $${pv_task_succeeded}
                        steps:
                          - mark_pv_done:
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

                - next_pv_task:
                    assign:
                      - i: $${i + 1}
                    next: extract_pv_loop

                - extract_weather:
                    steps:
                      - log_weather_start:
                          call: sys.log
                          args:
                            data: $${"Starting parallel weather extraction (" + string(len(weather_tasks)) + " tasks)"}
                      - extract_weather_parallel:
                          parallel:
                            for:
                              value: weather_task
                              in: $${weather_tasks}
                              steps:
                                - run_weather_task:
                                    try:
                                      steps:
                                        - run_weather_extract:
                                            call: googleapis.run.v2.projects.locations.jobs.run
                                            args:
                                              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                              body:
                                                overrides:
                                                  containerOverrides:
                                                    - env: $${weather_task}
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
                                    retry:
                                      predicate: $${retry_predicate}
                                      max_retries: 3
                                      backoff:
                                        initial_delay: 60
                                        max_delay: 600
                                        multiplier: 2
                                    # See PV-task except above. Weather tasks
                                    # aren't checkpointed, so on swallow we just
                                    # log and let the parallel branch end.
                                    except:
                                      as: task_err
                                      steps:
                                        - extract_weather_exit_code:
                                            assign:
                                              - failed_exit_code: $${default(map.get(task_err, ["data", "exit_code"]), 1)}
                                        - branch_on_weather_exit_code:
                                            switch:
                                              - condition: $${failed_exit_code == 2}
                                                raise: $${task_err}
                                        - log_weather_task_failure:
                                            call: sys.log
                                            args:
                                              data: '$${"Weather task failed (exit_code=" + string(failed_exit_code) + "), continuing: " + json.encode_to_string(task_err)}'
                                              severity: "WARNING"

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
    # container never ran). The exit code drives the per-task except's
    # decision to swallow (1: task failed, workflow continues) or
    # re-raise (2: WorkflowTerminatingError -- workflow aborts before the
    # commit step, leaving the cursor unchanged for tomorrow).
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
