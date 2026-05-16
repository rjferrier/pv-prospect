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
  call_log_level      = "LOG_ERRORS_ONLY"
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
              - index_object: $${"tracking/manifests/" + run_date + "/" + workflow_name + ".json"}

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

                # ---------- 2. execute the v2 phased manifest ----------
                # The index document carries one descriptor per phase:
                # {files, common_env, task_keys}. We iterate the
                # phases sequentially (clean -> prepare -> assemble);
                # within each phase, the chunk files are dispatched in
                # parallel. Each chunk dispatch is *one* Cloud Run job
                # execution that processes every unit listed in that
                # chunk file in-container — so the workflow's step
                # count scales with chunk count (hundreds), not unit
                # count (tens of thousands). Cloud Workflows caps
                # executions at 100K steps; per-unit dispatch blew past
                # that on the weather-grid backfill at ~40K units total.
                - fetch_manifest_index:
                    call: googleapis.storage.v1.objects.get
                    args:
                      bucket: $${bucket}
                      object: $${text.url_encode(index_object)}
                      alt: media
                    result: manifest_index_raw
                - decode_manifest_index:
                    assign:
                      - manifest_index: $${json.decode(manifest_index_raw)}
                      - phase_descriptors: $${manifest_index.phases}
                      - phase_index: 0

                - execute_phases:
                    for:
                      value: phase_descriptor
                      in: $${phase_descriptors}
                      steps:
                        - dispatch_chunks:
                            parallel:
                              for:
                                value: phase_part_file
                                in: $${phase_descriptor.files}
                                steps:
                                  - run_chunk:
                                      try:
                                        steps:
                                          - start_chunk_job:
                                              call: googleapis.run.v2.projects.locations.jobs.run
                                              args:
                                                name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                                body:
                                                  overrides:
                                                    containerOverrides:
                                                      - env:
                                                          - name: JOB_TYPE
                                                            value: process_transform_chunk
                                                          - name: WORKFLOW_NAME
                                                            value: $${workflow_name}
                                                          - name: RUN_DATE
                                                            value: $${run_date}
                                                          - name: INDEX_FILE
                                                            value: $${index_object}
                                                          - name: PHASE_INDEX
                                                            value: $${string(phase_index)}
                                                          - name: CHUNK_FILE
                                                            value: $${"tracking/manifests/" + run_date + "/" + phase_part_file}
                                              result: chunk_op
                                          - wait_chunk_op:
                                              call: wait_for_operation
                                              args:
                                                operation_name: $${chunk_op.name}
                                              result: chunk_op_done
                                          - wait_chunk:
                                              call: await_job
                                              args:
                                                execution_name: $${chunk_op_done.response.name}
                                      # Per-chunk failure handling matches the old
                                      # per-task except: exit 2 (WorkflowTerminating)
                                      # re-raises so the commit step is skipped and
                                      # the marker stays put; exit 1 (or unknown)
                                      # logs and swallows so the rest of the phase's
                                      # chunks still complete. Per-unit failures
                                      # inside the chunk are already swallowed by
                                      # the container — they surface only in the
                                      # ledger as 'failed' entries, not in the
                                      # chunk's exit code.
                                      except:
                                        as: chunk_err
                                        steps:
                                          - extract_exit_code:
                                              assign:
                                                - failed_exit_code: $${default(map.get(chunk_err, ["data", "exit_code"]), 1)}
                                          - branch_on_exit_code:
                                              switch:
                                                - condition: $${failed_exit_code == 2}
                                                  raise: $${chunk_err}
                                          - log_chunk_failure:
                                              call: sys.log
                                              args:
                                                data: '$${"Chunk failed (exit_code=" + string(failed_exit_code) + "), continuing: " + json.encode_to_string(chunk_err)}'
                                                severity: "WARNING"
                        - bump_phase_index:
                            assign:
                              - phase_index: $${phase_index + 1}

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
