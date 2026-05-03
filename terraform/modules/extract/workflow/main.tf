# Cloud Workflows orchestration for data extraction
#
# The workflow accepts parameters (sites, date range, source descriptors)
# and fans out Cloud Run Job executions — one per site/date combination.

resource "google_workflows_workflow" "data_extraction" {
  name                = "pv-prospect-extract"
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
  call_log_level      = "LOG_ALL_CALLS"
  description         = "Orchestrates daily PV Prospect data extraction via Cloud Run Jobs"

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
              - workflow_name: "pv-prospect-extract"
              - plan_job_type: "plan_extract"
              - pv_model_data_sources: $${default(map.get(args, "pv_model_data_sources"), ${jsonencode(var.default_pv_model_data_sources)})}
              - weather_model_data_sources: $${default(map.get(args, "weather_model_data_sources"), ${jsonencode(var.default_weather_model_data_sources)})}
              - date: $${default(map.get(args, "date"), default(map.get(args, "start_date"), text.substring(time.format(sys.now() - 86400), 0, 10)))}
              - raw_pv_system_ids: $${default(map.get(args, "pv_system_ids"), [])}
              - raw_locations: $${default(map.get(args, "locations"), [])}
              - dry_run: $${default(map.get(args, "dry_run"), "false")}
              - split_by: $${default(map.get(args, "split_by"), "${var.default_split_by}")}

        - parse_all_pvs:
            switch:
              - condition: $${json.encode_to_string(raw_pv_system_ids) == "\"all\""}
                assign:
                  - pv_system_ids: ${jsonencode(var.default_pv_system_ids)}
              - condition: true
                assign:
                  - pv_system_ids: $${raw_pv_system_ids}

        - parse_all_locations:
            switch:
              - condition: $${json.encode_to_string(raw_locations) == "\"all\""}
                assign:
                  - locations: ${jsonencode(var.default_locations)}
              - condition: true
                assign:
                  - locations: $${raw_locations}

        - run_plan:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: JOB_TYPE
                          value: $${plan_job_type}
                        - name: WORKFLOW_NAME
                          value: $${workflow_name}
                        - name: START_DATE
                          value: $${date}
                        - name: DATE
                          value: $${date}
                        - name: PV_SYSTEM_IDS
                          value: $${json.encode_to_string(pv_system_ids)}
                        - name: LOCATIONS
                          value: $${json.encode_to_string(locations)}
                        - name: PV_MODEL_DATA_SOURCES
                          value: $${json.encode_to_string(pv_model_data_sources)}
                        - name: WEATHER_MODEL_DATA_SOURCES
                          value: $${json.encode_to_string(weather_model_data_sources)}
                        - name: DRY_RUN
                          value: $${string(dry_run)}
                        - name: SPLIT_BY
                          value: $${split_by}
            result: plan_op

        - wait_plan_op:
            call: wait_for_operation
            args:
              operation_name: $${plan_op.name}
            result: plan_op_done

        - wait_plan:
            call: await_job
            args:
              execution_name: $${plan_op_done.response.name}

        - fetch_manifest:
            call: googleapis.storage.v1.objects.get
            args:
              bucket: $${bucket}
              object: $${text.url_encode("manifests/" + workflow_name + "_" + date + ".json")}
              alt: media
            result: manifest_raw

        - decode_manifest:
            assign:
              - manifest: $${json.decode(manifest_raw)}
              - phases: $${manifest.phases}

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
                - continue_phase:
                    assign:
                      - _dummy: true

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
                raise:
                  message: '$${"Job execution failed or was cancelled (succeeded: " + string(default(exec.succeededCount, 0)) + "/" + string(exec.taskCount) + ")"}'
                  data: $${exec}
  YAML
}
