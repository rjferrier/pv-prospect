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
                                      - name: DATE
                                        value: $${date}
                          result: clean_weather_result
                - clean_pvoutput_branch:
                    steps:
                      - run_clean_pvoutput_loop:
                          parallel:
                            for:
                              value: pv_system_id
                              in: $${pv_system_ids}
                              steps:
                                - run_clean_pvoutput:
                                    call: googleapis.run.v2.projects.locations.jobs.run
                                    args:
                                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                      body:
                                        overrides:
                                          containerOverrides:
                                            - env:
                                                - name: TRANSFORM_STEP
                                                  value: clean_pvoutput
                                                - name: PV_SYSTEM_ID
                                                  value: $${string(pv_system_id)}
                                                - name: DATE
                                                  value: $${date}
                                    result: clean_pvoutput_result

        - process_parallel:
            parallel:
              branches:
                - process_weather_branch:
                    steps:
                      - run_process_weather:
                          call: googleapis.run.v2.projects.locations.jobs.run
                          args:
                            name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                            body:
                              overrides:
                                containerOverrides:
                                  - env:
                                      - name: TRANSFORM_STEP
                                        value: process_weather
                                      - name: DATE
                                        value: $${date}
                          result: process_weather_result
                - process_pv_branch:
                    steps:
                      - run_process_pv_loop:
                          parallel:
                            for:
                              value: pv_system_id
                              in: $${pv_system_ids}
                              steps:
                                - run_process_pv:
                                    call: googleapis.run.v2.projects.locations.jobs.run
                                    args:
                                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
                                      body:
                                        overrides:
                                          containerOverrides:
                                            - env:
                                                - name: TRANSFORM_STEP
                                                  value: process_pv
                                                - name: PV_SYSTEM_ID
                                                  value: $${string(pv_system_id)}
                                                - name: DATE
                                                  value: $${date}
                                    result: process_pv_result

        - done:
            return: "completed"
  YAML
}
