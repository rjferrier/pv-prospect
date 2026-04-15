# Cloud Workflows orchestration for data extraction
#
# The workflow accepts parameters (sites, date range, source descriptors)
# and fans out Cloud Run Job executions — one per site/date combination.

resource "google_workflows_workflow" "data_extraction" {
  name                = "pv-prospect-extract"
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
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
              - workflow_name: "pv-prospect-extract"
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

        - preprocess:
            parallel:
              for:
                value: ds
                in: $${pv_model_data_sources}
                steps:
                  - log_preprocess:
                      call: sys.log
                      args:
                        data: $${"Starting preprocess for data source " + ds}
                  - run_preprocess:
                      call: googleapis.run.v2.projects.locations.jobs.run
                      args:
                        name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                        body:
                          overrides:
                            containerOverrides:
                              - env:
                                  - name: JOB_TYPE
                                    value: preprocess
                                  - name: DATA_SOURCE
                                    value: $${ds}
                                  - name: WORKFLOW_NAME
                                    value: $${workflow_name}
                      result: preprocess_result
                  - wait_preprocess:
                      call: await_job
                      args:
                        execution_name: $${preprocess_result.name}

        - extract:
            parallel:
              branches:
                - extract_for_pv_model:
                    steps:
                      - extract_pv_loop:
                          parallel:
                            for:
                              value: pv_system_id
                              in: $${pv_system_ids}
                              steps:
                                - extract_per_source:
                                    for:
                                      value: ds
                                      in: $${pv_model_data_sources}
                                      steps:
                                        - log_pv_extract:
                                            call: sys.log
                                            args:
                                              data: $${"Extracting PV data for system " + string(pv_system_id) + " (" + ds + ") for " + date}
                                        - run_extract:
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
                                                          value: $${ds}
                                                        - name: PV_SYSTEM_ID
                                                          value: $${string(pv_system_id)}
                                                        - name: DATE
                                                          value: $${date}
                                                        - name: START_DATE
                                                          value: $${date}
                                                        - name: DRY_RUN
                                                          value: $${dry_run}
                                                        - name: SPLIT_BY
                                                          value: $${split_by}
                                                        - name: WORKFLOW_NAME
                                                          value: $${workflow_name}
                                            result: extract_result
                                        - wait_pv_extract:
                                            call: await_job
                                            args:
                                              execution_name: $${extract_result.name}
                - extract_for_weather_model:
                    steps:
                      - extract_location_loop:
                          parallel:
                            for:
                              value: location
                              in: $${locations}
                              steps:
                                - extract_location_per_source:
                                    for:
                                      value: ds
                                      in: $${weather_model_data_sources}
                                      steps:
                                        - log_weather_extract:
                                            call: sys.log
                                            args:
                                              data: $${"Extracting weather data for location " + location + " (" + ds + ") for " + date}
                                        - run_extract_location:
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
                                                          value: $${ds}
                                                        - name: LOCATION
                                                          value: $${location}
                                                        - name: DATE
                                                          value: $${date}
                                                        - name: START_DATE
                                                          value: $${date}
                                                        - name: DRY_RUN
                                                          value: $${dry_run}
                                                        - name: SPLIT_BY
                                                          value: $${split_by}
                                                        - name: WORKFLOW_NAME
                                                          value: $${workflow_name}
                                            result: extract_location_result
                                        - wait_weather_extract:
                                            call: await_job
                                            args:
                                              execution_name: $${extract_location_result.name}

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
            result: consolidate_logs_result

        - wait_consolidate:
            call: await_job
            args:
              execution_name: $${consolidate_logs_result.name}

        - done:
            return: "completed"

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
