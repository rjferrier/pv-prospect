# Cloud Workflows orchestration for data extraction
#
# The workflow accepts parameters (sites, date range, source descriptors)
# and fans out Cloud Run Job executions — one per site/date combination.

resource "google_workflows_workflow" "data_extraction" {
  name            = "pv-prospect-extract"
  region          = var.region
  service_account = var.service_account_email
  description     = "Orchestrates daily PV Prospect data extraction via Cloud Run Jobs"

  source_contents = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - project_id: $${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
              - region: "${var.region}"
              - job_name: "${var.cloud_run_job_name}"
              - source_descriptors: $${default(map.get(args, "source_descriptors"), ${jsonencode(var.default_source_descriptors)})}
              - start_date: $${default(map.get(args, "start_date"), text.substring(time.format(sys.now()), 0, 10))}
              - end_date: $${default(map.get(args, "end_date"), text.substring(time.format(sys.now()), 0, 10))}
              - pv_system_ids: $${default(map.get(args, "pv_system_ids"), ${jsonencode(var.default_pv_system_ids)})}
              - overwrite: $${default(map.get(args, "overwrite"), "false")}
              - dry_run: $${default(map.get(args, "dry_run"), "false")}
              - by_week: $${default(map.get(args, "by_week"), "${var.default_by_week}")}

        - preprocess:
            parallel:
              for:
                value: sd
                in: $${source_descriptors}
                steps:
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
                                  - name: SOURCE_DESCRIPTOR
                                    value: $${sd}
                      result: preprocess_result

        - extract:
            parallel:
              for:
                value: pv_system_id
                in: $${pv_system_ids}
                steps:
                  - extract_per_source:
                      for:
                        value: sd
                        in: $${source_descriptors}
                        steps:
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
                                          - name: SOURCE_DESCRIPTOR
                                            value: $${sd}
                                          - name: PV_SYSTEM_ID
                                            value: $${string(pv_system_id)}
                                          - name: START_DATE
                                            value: $${start_date}
                                          - name: END_DATE
                                            value: $${end_date}
                                          - name: OVERWRITE
                                            value: $${overwrite}
                                          - name: DRY_RUN
                                            value: $${dry_run}
                                          - name: BY_WEEK
                                            value: $${string(by_week)}
                              result: extract_result

        - done:
            return: "completed"
  YAML
}
