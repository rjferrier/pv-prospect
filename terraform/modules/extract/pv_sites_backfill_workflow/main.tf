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
#   3. Load a GCS position checkpoint {"next_pv_task_index": N} so a
#      re-triggered run resumes at PV task N rather than re-running
#      already-attempted ones. Per-site ledger entries from the container
#      handle within-task idempotency for the parallel weather phase.
#   4. Dispatch PV extraction jobs *sequentially* per PV system (PVOutput
#      rate-limits at 300 requests/hour; 10 sites × 28 days = 280 calls).
#      The checkpoint advances after each dispatch (success or exit-1
#      crash), so the workflow marches forward at a predictable cadence.
#   5. Dispatch weather extraction jobs in *parallel* per PV system (OpenMeteo
#      rate limits are generous enough for 10–20 concurrent calls).
#   6. Invoke the Cloud Run Job with JOB_TYPE=commit_pv_site_backfill, which
#      advances the cursor so tomorrow's run picks up from the next window.
#   7. Delete the checkpoint (successful run) and consolidate logs.
#
# Failures branch on the container exit code surfaced by await_job:
#   exit 1 (container crash)             -- log, swallow, continue. The
#       checkpoint advances and the cursor commit at the end still runs,
#       so tomorrow's run moves on rather than retrying the failure.
#       Per-site outcomes (success or fail) are recorded in the ledger
#       inside the container; transiently-failed sites become small
#       per-window holes rather than perpetual retries.
#   exit 2 (WorkflowTerminatingError)    -- re-raise. The outer
#       run_pipeline try aborts before the commit step, leaving the
#       cursor unchanged for tomorrow. consolidate_logs still runs.

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
              - manifest_object: $${"tracking/manifests/" + run_date + "/" + workflow_name + ".json"}
              - checkpoint_object: "${var.checkpoint_object_path}"
              - pv_system_ids: $${default(map.get(args, "pv_system_ids"), ${jsonencode(var.default_pv_system_ids)})}
              - pv_data_source: $${default(map.get(args, "pv_data_source"), "${var.pv_data_source}")}
              - weather_data_source: $${default(map.get(args, "weather_data_source"), "${var.weather_data_source}")}
              - dry_run: $${default(map.get(args, "dry_run"), "false")}
              - next_pv_task_index: 0

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

                # Load the position checkpoint so a re-triggered run resumes
                # at the PV task index where the previous run stopped. The
                # object stores {"next_pv_task_index": N}. 404 (no prior
                # run) is treated as N=0. The parallel weather phase is not
                # checkpointed; per-site ledger filtering inside each
                # weather container handles same-day re-trigger redundancy.
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
                              - next_pv_task_index: $${default(map.get(json.decode(checkpoint_raw), "next_pv_task_index"), 0)}
                        - log_resuming:
                            call: sys.log
                            args:
                              data: $${"Resuming from checkpoint — starting at PV task " + string(next_pv_task_index)}
                    except:
                      as: checkpoint_err
                      steps:
                        - init_next_index:
                            assign:
                              - next_pv_task_index: 0

                - extract_pv_init:
                    assign:
                      - i: $${next_pv_task_index}
                - extract_pv_loop:
                    switch:
                      - condition: $${i >= len(pv_tasks)}
                        next: extract_weather
                - get_pv_task:
                    assign:
                      - pv_task: $${pv_tasks[i]}
                - log_pv_start:
                    call: sys.log
                    args:
                      data: $${"Extracting PV task " + string(i+1) + "/" + string(len(pv_tasks))}

                # With the container's per-site idempotency and exit-0-on-
                # site-failure policy, the workflow no longer retries at
                # this layer. Exit 1 (container crash) is logged and the
                # checkpoint still advances. Exit 2 re-raises so the cursor
                # commit is skipped.
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
                              data: '$${"PV task crashed (exit_code=" + string(failed_exit_code) + "), advancing anyway: " + json.encode_to_string(task_err)}'
                              severity: "WARNING"

                # Advance the position checkpoint unconditionally after
                # every dispatched PV task. Per-site outcomes are in the
                # ledger; the workflow's role here is just progress
                # tracking for same-day re-triggers.
                - advance_pv_checkpoint:
                    call: http.post
                    args:
                      url: $${"https://storage.googleapis.com/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=" + text.url_encode(checkpoint_object)}
                      auth:
                        type: OAuth2
                      headers:
                        Content-Type: application/json
                      body: '$${"{\"next_pv_task_index\": " + string(i + 1) + "}"}'

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
                                    # Same exit-code policy as the PV phase.
                                    # No workflow-level retry; per-site
                                    # idempotency lives in the container.
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
                                              data: '$${"Weather task crashed (exit_code=" + string(failed_exit_code) + "), parallel branch ending: " + json.encode_to_string(task_err)}'
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
