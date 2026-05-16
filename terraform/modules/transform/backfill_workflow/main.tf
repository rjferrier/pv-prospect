# Cloud Workflows orchestration for the transform backfill.
#
# A single, scope-parameterised module; instantiate twice (once for each
# BackfillScope: pv_sites and weather_grid). The workflow plans from the
# corresponding extraction backfill's committed task-outcome ledger — it
# transforms exactly what extraction recorded as harvested, so it cannot
# drift onto a window extraction never produced raw data for.
#
# Execution flow:
#
#   1. plan_transform_backfill: reads the scope's consumed-through marker,
#      takes the next MAX_EXTRACT_RUNS unconsumed extraction consolidated
#      ledgers, and writes the v2 phased orchestrator manifest — an
#      index document at <workflow>.json plus one
#      <workflow>.phase-<N>.json per phase (alongside a
#      <workflow>.marker.json next-marker sidecar).
#   2. Read the index, then per-phase: fetch the phase file and dispatch
#      clean -> prepare -> assemble tasks in parallel within each phase.
#      Each task's env is reconstructed at dispatch time as the phase's
#      hoisted common_env plus a varying-fields suffix zipped from
#      task_keys + the row. This keeps any single per-step HTTP fetch
#      under the Workflows 2 MiB response limit — the v1 single-doc
#      shape blew past it for the weather-grid backfill (~10 K tasks
#      per phase, ~10 MB total).
#   3. commit_transform_backfill: promotes the sidecar's next_marker to
#      the live marker.
#
# Failures branch on the container exit code surfaced by await_job:
#   exit 1 (container crash)          -- log, swallow, continue. The
#       commit step still runs, so the marker advances and a
#       transiently-failed task becomes a recorded hole rather than a
#       perpetual retry.
#   exit 2 (WorkflowTerminatingError) -- re-raise. The run_pipeline try
#       aborts before the commit step, leaving the marker unchanged so
#       the next run re-derives the same plan. consolidate_logs still runs.

locals {
  workflow_name = "pv-prospect-transform-${var.workflow_name_suffix}-backfill"
}

resource "google_workflows_workflow" "transform_backfill" {
  name                = local.workflow_name
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"
  description         = "Orchestrates the ${var.backfill_scope} transform backfill, planning from the extraction backfill's ledger"

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
              # orchestrator manifest and its next-marker sidecar at this
              # run_date.
              - run_date: $${text.substring(time.format(sys.now()), 0, 10)}

        - run_pipeline:
            try:
              steps:
                # ---------- 1. plan_transform_backfill ----------
                # Plans from the extraction backfill's consolidated ledger
                # and writes the orchestrator manifest directly.
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
                                - name: MAX_EXTRACT_RUNS
                                  value: "${var.max_extract_runs}"
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

                # ---------- 2. fetch + execute the v2 phased manifest ----------
                # The index document carries one descriptor per phase:
                # {file, common_env, task_keys}. We fetch each phase
                # file (just {rows}) inside the loop so no single
                # HTTP response exceeds the Workflows 2 MiB limit.
                - fetch_manifest_index:
                    call: googleapis.storage.v1.objects.get
                    args:
                      bucket: $${bucket}
                      object: $${text.url_encode("tracking/manifests/" + run_date + "/" + workflow_name + ".json")}
                      alt: media
                    result: manifest_index_raw
                - decode_manifest_index:
                    assign:
                      - manifest_index: $${json.decode(manifest_index_raw)}
                      - phase_descriptors: $${manifest_index.phases}

                - execute_phases:
                    for:
                      value: phase_descriptor
                      in: $${phase_descriptors}
                      steps:
                        - fetch_phase_file:
                            call: googleapis.storage.v1.objects.get
                            args:
                              bucket: $${bucket}
                              object: $${text.url_encode("tracking/manifests/" + run_date + "/" + phase_descriptor.file)}
                              alt: media
                            result: phase_doc_raw
                        - decode_phase_file:
                            assign:
                              - phase_doc: $${json.decode(phase_doc_raw)}
                              - rows: $${phase_doc.rows}
                              - phase_common_env: $${phase_descriptor.common_env}
                              - phase_task_keys: $${phase_descriptor.task_keys}
                        - check_phase_empty:
                            switch:
                              - condition: $${len(rows) == 0}
                                next: continue_phase
                        - dispatch_tasks:
                            parallel:
                              for:
                                value: row
                                in: $${rows}
                                steps:
                                  - build_task_env:
                                      call: expand_task_env
                                      args:
                                        common_env: $${phase_common_env}
                                        task_keys: $${phase_task_keys}
                                        row: $${row}
                                      result: task_env
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
                                      # No workflow-level retry — in line with
                                      # the extraction backfills. Branch on the
                                      # container exit code surfaced by await_job:
                                      # 2 -> WorkflowTerminatingError, re-raise so
                                      # the run_pipeline try aborts before the
                                      # commit step, leaving the marker unchanged.
                                      # 1 (or unknown) -> log and swallow so the
                                      # run still reaches commit (predictable
                                      # cadence; the ledger records the failure).
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
                                                data: '$${"Task failed (exit_code=" + string(failed_exit_code) + "), continuing: " + json.encode_to_string(task_err)}'
                                                severity: "WARNING"
                        - continue_phase:
                            assign:
                              - _dummy: true

                # ---------- 3. commit_transform_backfill ----------
                # Promotes the next-marker sidecar to the live marker.
                # Reached after every exit-1 failure is swallowed, so it
                # runs on partial-failure runs too; an exit-2 re-raise
                # above skips it, leaving the marker unchanged.
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

        # ---------- 4. consolidate_logs (always runs) ----------
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

    # Reconstructs a Cloud Run env list from the v2 phased manifest's
    # (common_env, task_keys, row) tuple: starts with the phase-level
    # constants and appends one {name, value} pair per non-null cell
    # in the positional row, aligned to task_keys. A null cell signals
    # "this task does not carry this env-var" — emitting an empty
    # string would change the absent-key semantics the container
    # currently relies on, so we skip the entry entirely.
    #
    # The {name, value} entry is built via an `assign` step (YAML map
    # syntax) rather than an inline `{...}` expression, since the
    # Workflows expression parser only accepts list/primitive literals,
    # not map literals, inside an expression body.
    expand_task_env:
      params: [common_env, task_keys, row]
      steps:
        - init:
            assign:
              - task_extras: []
              - idx: 0
        - extras_loop:
            for:
              value: row_value
              in: $${row}
              steps:
                - maybe_append:
                    switch:
                      - condition: $${row_value != null}
                        steps:
                          - build_entry:
                              assign:
                                - new_entry:
                                    name: $${task_keys[idx]}
                                    value: $${row_value}
                          - append_extra:
                              assign:
                                - task_extras: $${list.concat(task_extras, [new_entry])}
                - bump_idx:
                    assign:
                      - idx: $${idx + 1}
        - return_env:
            return: $${list.concat(common_env, task_extras)}

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
    # commit step, leaving the marker unchanged for the next run).
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
