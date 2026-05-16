# Cloud Workflows orchestration for the daily weather grid backfill.
#
# This workflow is manifest-driven and runs in a single execution per day:
#
#   1. It invokes the Cloud Run Job with JOB_TYPE=plan_weather_grid_backfill,
#      which writes a JSON manifest to GCS describing the day's work. Each
#      batch's env carries the resolved LOCATIONS list for one
#      (sample_file × window) pair; per-site ledger entries are recorded by
#      the container as it iterates.
#   2. It reads the manifest from GCS to obtain the ordered list of batches.
#   3. It dispatches one extract_and_load Cloud Run Job per batch from i=0
#      to len(batches)-1, sleeping `sleep_seconds_between_batches` between
#      successive dispatches (skipped before the first). The in-batch sleep
#      is what keeps the workflow under OpenMeteo's 5,000/hour limit: with
#      9 batches × ~1,330 calls each and a 12-min sleep, a sliding 60-min
#      window contains at most 3 batches ≈ 3,990 calls.
#   4. After every batch in the manifest has been attempted, it invokes the
#      Cloud Run Job with JOB_TYPE=commit_weather_grid_backfill, which
#      advances the live cursor so tomorrow's run picks up from the next
#      point in the backfill.
#   5. consolidate_logs runs unconditionally — even after a swallowed
#      per-batch exit-1 crash or a workflow-level error — so the ledger is
#      always finalised.

resource "google_workflows_workflow" "weather_grid_backfill" {
  name                = "pv-prospect-extract-weather-grid-backfill"
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
  call_log_level      = "LOG_ERRORS_ONLY"
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
              - manifest_object: $${"tracking/manifests/" + run_date + "/" + workflow_name + ".json"}
              - data_source: $${default(map.get(args, "data_source"), "${var.data_source}")}
              - sleep_seconds: $${default(map.get(args, "sleep_seconds_between_batches"), ${var.sleep_seconds_between_batches})}
              - dry_run: $${default(map.get(args, "dry_run"), "false")}

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

                - dispatch_init:
                    assign:
                      - i: 0

                - dispatch_loop:
                    switch:
                      - condition: $${i >= len(batches)}
                        next: commit

                - get_batch:
                    assign:
                      - batch: $${batches[i]}

                # Sleep before every batch except the first. With
                # ``sleep_seconds`` set to the OpenMeteo per-batch pacing
                # constant (12 min by default), this single in-batch
                # sleep is what enforces the 5,000/hour rate limit —
                # there is no cross-execution coordination, so the loop
                # must space its own dispatches.
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
                      data: $${"Starting weather extraction for batch " + string(i+1) + "/" + string(len(batches))}

                # With the container's per-site idempotency and exit-0-on-
                # site-failure policy, the workflow no longer retries at
                # this layer. Exit 1 (container crash) is logged and the
                # loop advances to the next batch. Exit 2
                # (WorkflowTerminatingError) re-raises out of run_pipeline
                # so the cursor commit is skipped.
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

                - next_batch:
                    assign:
                      - i: $${i + 1}
                    next: dispatch_loop

                # Commit once every batch in the manifest has been attempted
                # (success or exit-1 swallowed crash). Exit-2 failures never
                # reach here — they re-raise out of run_batch and abort
                # run_pipeline, skipping the commit, and consolidate_logs
                # still runs because it lives outside the try.
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
