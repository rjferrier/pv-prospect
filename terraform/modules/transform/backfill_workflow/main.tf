# Cloud Workflows orchestration for the daily transform backfill.
#
# A single, scope-parameterised module; instantiate twice (once for each
# BackfillScope: pv_sites and weather_grid). The workflow trails the
# corresponding extraction-side backfill, transforming the same date
# window the extraction backfill produced raw data for.
#
# Execution flow:
#
#   1. plan_transform_backfill: writes today's date-window plan + next
#      cursor to the scope's backfill manifest in GCS.
#   2. Read that manifest to obtain start_date and end_date.
#   3. plan_transform: builds the orchestrator phased manifest for
#      [start_date, end_date) and the chosen PV systems / locations.
#   4. Iterate phases, dispatching clean → prepare → assemble tasks in
#      parallel within each phase (same shape as the daily transform).
#   5. commit_transform_backfill: promotes the next-cursor to the live
#      cursor only if all transform jobs succeeded.
#   6. consolidate_logs.
#
# If any transform job fails the cursor is not advanced, so tomorrow's
# scheduled run replans the same window. The orchestrator's task-outcome
# ledger ensures already-completed tasks are skipped on re-run.

locals {
  workflow_name = "pv-prospect-transform-${var.workflow_name_suffix}-backfill"

  # Scope-specific selector for what the orchestrator manifests cover.
  # Empty for the inapplicable scope so the daily-transform planning
  # logic emits no spurious tasks of the wrong kind.
  pv_system_ids_json = var.backfill_scope == "pv_sites" ? jsonencode(var.default_pv_system_ids) : "[]"
  locations_json     = var.backfill_scope == "weather_grid" ? jsonencode(var.default_locations) : "[]"
}

resource "google_workflows_workflow" "transform_backfill" {
  name                = local.workflow_name
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"
  description         = "Orchestrates daily ${var.backfill_scope} transform backfill via plan-commit cursor"

  source_contents = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - project_id: $${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
              - region: "${var.region}"
              - job_name: "${var.cloud_run_job_name}"
              - bucket: "${var.staging_bucket_name}"
              - workflow_name: "${local.workflow_name}"
              - backfill_scope: "${var.backfill_scope}"
              # Workflow trigger date (UTC), pinned once and propagated to every
              # task as RUN_DATE. The plan_transform_backfill job writes the
              # backfill manifest at this run_date; the orchestrator manifest
              # from plan_transform lands under the same run_date.
              - run_date: $${text.substring(time.format(sys.now()), 0, 10)}
              - backfill_manifest_object: $${"tracking/manifests/" + run_date + "/" + workflow_name + ".backfill.json"}
              - pv_system_ids: ${local.pv_system_ids_json}
              - locations: ${local.locations_json}

        - run_pipeline:
            try:
              steps:
                # ---------- 1. plan_transform_backfill ----------
                - run_plan_backfill:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                      body:
                        overrides:
                          containerOverrides:
                            - env:
                                - name: JOB_TYPE
                                  value: plan_transform_backfill
                                - name: BACKFILL_SCOPE
                                  value: $${backfill_scope}
                                - name: WORKFLOW_NAME
                                  value: $${workflow_name}
                                - name: RUN_DATE
                                  value: $${run_date}
                    result: plan_backfill_op
                - wait_plan_backfill_op:
                    call: wait_for_operation
                    args:
                      operation_name: $${plan_backfill_op.name}
                    result: plan_backfill_op_done
                - wait_plan_backfill:
                    call: await_job
                    args:
                      execution_name: $${plan_backfill_op_done.response.name}

                # ---------- 2. fetch the window from the backfill manifest ----------
                - fetch_backfill_manifest:
                    call: googleapis.storage.v1.objects.get
                    args:
                      bucket: $${bucket}
                      object: $${text.url_encode(backfill_manifest_object)}
                      alt: media
                    result: backfill_manifest_raw
                - decode_backfill_manifest:
                    assign:
                      - backfill_manifest: $${json.decode(backfill_manifest_raw)}
                      - start_date: $${backfill_manifest.start_date}
                      - end_date: $${backfill_manifest.end_date}

                # ---------- 3. plan_transform across the window ----------
                - run_plan_transform:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                      body:
                        overrides:
                          containerOverrides:
                            - env:
                                - name: JOB_TYPE
                                  value: plan_transform
                                - name: WORKFLOW_NAME
                                  value: $${workflow_name}
                                - name: RUN_DATE
                                  value: $${run_date}
                                - name: START_DATE
                                  value: $${start_date}
                                - name: END_DATE
                                  value: $${end_date}
                                - name: DATE
                                  value: $${start_date}
                                - name: PV_SYSTEM_IDS
                                  value: $${json.encode_to_string(pv_system_ids)}
                                - name: LOCATIONS
                                  value: $${json.encode_to_string(locations)}
                                - name: SPLIT_BY
                                  value: day
                    result: plan_transform_op
                - wait_plan_transform_op:
                    call: wait_for_operation
                    args:
                      operation_name: $${plan_transform_op.name}
                    result: plan_transform_op_done
                - wait_plan_transform:
                    call: await_job
                    args:
                      execution_name: $${plan_transform_op_done.response.name}

                # ---------- 4. fetch + execute the orchestrator manifest ----------
                # The orchestrator phases manifest lives at
                # tracking/manifests/<run_date>/<workflow>.json (alongside
                # the .backfill.json date-window plan above).
                - fetch_orchestrator_manifest:
                    call: googleapis.storage.v1.objects.get
                    args:
                      bucket: $${bucket}
                      object: $${text.url_encode("tracking/manifests/" + run_date + "/" + workflow_name + ".json")}
                      alt: media
                    result: orchestrator_manifest_raw
                - decode_orchestrator_manifest:
                    assign:
                      - orchestrator_manifest: $${json.decode(orchestrator_manifest_raw)}
                      - phases: $${orchestrator_manifest.phases}

                - execute_phases:
                    for:
                      value: phase_tasks
                      in: $${phases}
                      steps:
                        - check_phase_empty:
                            switch:
                              - condition: $${len(phase_tasks) == 0}
                                next: continue_phase
                        - dispatch_tasks:
                            parallel:
                              for:
                                value: task_env
                                in: $${phase_tasks}
                                steps:
                                  - run_task:
                                      try:
                                        steps:
                                          - start_job:
                                              call: googleapis.run.v2.projects.locations.jobs.run
                                              args:
                                                name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                                body:
                                                  overrides:
                                                    containerOverrides:
                                                      - env: $${task_env}
                                              result: task_op
                                          - wait_task_op:
                                              call: wait_for_operation
                                              args:
                                                operation_name: $${task_op.name}
                                              result: task_op_done
                                          - wait_task:
                                              call: await_job
                                              args:
                                                execution_name: $${task_op_done.response.name}
                                      retry:
                                        predicate: $${retry_predicate}
                                        max_retries: 3
                                        backoff:
                                          initial_delay: 60
                                          max_delay: 600
                                          multiplier: 2
                                      # After retries are exhausted, branch on the
                                      # container exit code surfaced by await_job:
                                      # 2 -> WorkflowTerminatingError, re-raise so
                                      # the run_pipeline try aborts before the
                                      # commit step, leaving the cursor unchanged.
                                      # 1 (or unknown) -> log and swallow so the
                                      # cursor still advances despite the per-task
                                      # failure (ledger records the failure).
                                      except:
                                        as: task_err
                                        steps:
                                          - extract_exit_code:
                                              assign:
                                                - failed_exit_code: $${default(map.get(default(map.get(task_err, "data"), {}), "exit_code"), 1)}
                                          - branch_on_exit_code:
                                              switch:
                                                - condition: $${failed_exit_code == 2}
                                                  raise: $${task_err}
                                          - log_task_failure:
                                              call: sys.log
                                              args:
                                                data: '$${"Task failed (exit_code=" + string(failed_exit_code) + "), continuing: " + json.encode_to_string(task_err)}'
                                                severity: "WARNING"
                        - continue_phase:
                            assign:
                              - _dummy: true

                # ---------- 5. commit_transform_backfill ----------
                # Only reached if every phase succeeded — a failure in
                # the try block above skips this and falls through to
                # consolidate_logs, leaving the cursor unchanged for
                # tomorrow to retry.
                - run_commit_backfill:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                      body:
                        overrides:
                          containerOverrides:
                            - env:
                                - name: JOB_TYPE
                                  value: commit_transform_backfill
                                - name: BACKFILL_SCOPE
                                  value: $${backfill_scope}
                                - name: WORKFLOW_NAME
                                  value: $${workflow_name}
                                - name: RUN_DATE
                                  value: $${run_date}
                    result: commit_backfill_op
                - wait_commit_backfill_op:
                    call: wait_for_operation
                    args:
                      operation_name: $${commit_backfill_op.name}
                    result: commit_backfill_op_done
                - wait_commit_backfill:
                    call: await_job
                    args:
                      execution_name: $${commit_backfill_op_done.response.name}
            except:
              as: workflow_err
              steps:
                - log_workflow_error:
                    call: sys.log
                    args:
                      data: '$${"Backfill workflow encountered an error - proceeding to log consolidation. Error: " + json.encode_to_string(workflow_err)}'
                      severity: "ERROR"

        # ---------- 6. consolidate_logs (always runs) ----------
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
        - wait_consolidate_op:
            call: wait_for_operation
            args:
              operation_name: $${consolidate_logs_op.name}
            result: consolidate_logs_op_done
        - wait_consolidate:
            call: await_job
            args:
              execution_name: $${consolidate_logs_op_done.response.name}

        - done:
            return: "completed"

    retry_predicate:
      params: [e]
      steps:
        - extract_error_code:
            assign:
              - error_code: $${default(map.get(e, "code"), 0)}
        - check_retriable:
            switch:
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
    # container never ran). The exit code drives the outer except's
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
              - status_obj: $${default(map.get(first_task, "status"), {})}
              - last_attempt: $${default(map.get(status_obj, "lastAttemptResult"), {})}
              - exit_code: $${default(map.get(last_attempt, "exitCode"), 1)}
        - return_exit_code:
            return: $${exit_code}
  YAML
}
