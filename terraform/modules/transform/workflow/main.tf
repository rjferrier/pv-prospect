# Cloud Workflows orchestration for data transformation
#
# The workflow accepts parameters (sites, date) and orchestrates the DAG
# of Cloud Run Jobs to clean and parse the data.

resource "google_workflows_workflow" "data_transformation" {
  name            = "pv-prospect-transform"
  region          = var.region
  service_account = var.service_account_email
  description     = "Orchestrates PV Prospect data transformation DAG via Cloud Run Jobs"

  source_contents = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - project_id: $${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
              - region: "${var.region}"
              - job_name: "${var.cloud_run_job_name}"
              - date: $${default(map.get(args, "date"), text.substring(time.format(sys.now()), 0, 10))}
              - pv_system_ids: $${default(map.get(args, "pv_system_ids"), ${jsonencode(var.default_pv_system_ids)})}

        - clean_parallel:
            parallel:
              branches:
                - clean_weather_branch:
                    steps:
                      - run_clean_weather_loop:
                          parallel:
                            for:
                              value: pv_system_id
                              in: $${pv_system_ids}
                              steps:
                                - run_clean_weather:
                                    call: googleapis.run.v2.projects.locations.jobs.run
                                    args:
                                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                      body:
                                        overrides:
                                          containerOverrides:
                                            - env:
                                                - name: TRANSFORM_STEP
                                                  value: clean_weather
                                                - name: PV_SYSTEM_ID
                                                  value: $${string(pv_system_id)}
                                                - name: DATE
                                                  value: $${date}
                                    result: clean_weather_result
                - clean_pv_branch:
                    steps:
                      - run_clean_pv_loop:
                          parallel:
                            for:
                              value: pv_system_id
                              in: $${pv_system_ids}
                              steps:
                                - run_clean_pv:
                                    call: googleapis.run.v2.projects.locations.jobs.run
                                    args:
                                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                      body:
                                        overrides:
                                          containerOverrides:
                                            - env:
                                                - name: TRANSFORM_STEP
                                                  value: clean_pv
                                                - name: PV_SYSTEM_ID
                                                  value: $${string(pv_system_id)}
                                                - name: DATE
                                                  value: $${date}
                                    result: clean_pv_result

        - process_parallel:
            parallel:
              branches:
                - prepare_weather_branch:
                    steps:
                      - run_prepare_weather_loop:
                          parallel:
                            for:
                              value: pv_system_id
                              in: $${pv_system_ids}
                              steps:
                                - run_prepare_weather:
                                    call: googleapis.run.v2.projects.locations.jobs.run
                                    args:
                                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                      body:
                                        overrides:
                                          containerOverrides:
                                            - env:
                                                - name: TRANSFORM_STEP
                                                  value: prepare_weather
                                                - name: PV_SYSTEM_ID
                                                  value: $${string(pv_system_id)}
                                                - name: DATE
                                                  value: $${date}
                                    result: prepare_weather_result
                - prepare_pv_branch:
                    steps:
                      - run_prepare_pv_loop:
                          parallel:
                            for:
                              value: pv_system_id
                              in: $${pv_system_ids}
                              steps:
                                - run_prepare_pv:
                                    call: googleapis.run.v2.projects.locations.jobs.run
                                    args:
                                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                      body:
                                        overrides:
                                          containerOverrides:
                                            - env:
                                                - name: TRANSFORM_STEP
                                                  value: prepare_pv
                                                - name: PV_SYSTEM_ID
                                                  value: $${string(pv_system_id)}
                                                - name: DATE
                                                  value: $${date}
                                    result: prepare_pv_result

        - done:
            return: "completed"
  YAML
}
