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
#   3. Dispatch PV extraction jobs *sequentially* per PV system (PVOutput
#      rate-limits at 300 requests/hour; 10 sites × 28 days = 280 calls).
#   4. Dispatch weather extraction jobs in *parallel* per PV system (OpenMeteo
#      rate limits are generous enough for 10–20 concurrent calls).
#   5. Invoke the Cloud Run Job with JOB_TYPE=commit_pv_site_backfill, which
#      advances the cursor so tomorrow's run picks up from the next window.
#
# If any extraction job fails, the workflow aborts without committing the
# cursor, so tomorrow's plan job will re-derive the same manifest and retry.

resource "google_workflows_workflow" "pv_site_backfill" {
  name                = "pv-prospect-extract-pv-site-backfill"
  region              = var.region
  service_account     = var.service_account_email
  deletion_protection = false
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
              - workflow_name: "pv-prospect-extract-pv-site-backfill"
              - bucket: "${var.staging_bucket_name}"
              - manifest_object: "${var.manifest_object_path}"
              - pv_system_ids: $${default(map.get(args, "pv_system_ids"), ${jsonencode(var.default_pv_system_ids)})}
              - pv_data_source: $${default(map.get(args, "pv_data_source"), "${var.pv_data_source}")}
              - weather_data_source: $${default(map.get(args, "weather_data_source"), "${var.weather_data_source}")}
              - dry_run: $${default(map.get(args, "dry_run"), "false")}

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
            result: plan_result

        - wait_for_plan:
            call: await_job
            args:
              execution_name: $${plan_result.name}

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

        - extract_pv:
            for:
              value: pv_system_id
              in: $${pv_system_ids}
              steps:
                - run_pv_extract:
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
                                  value: $${pv_data_source}
                                - name: PV_SYSTEM_ID
                                  value: $${string(pv_system_id)}
                                - name: START_DATE
                                  value: $${manifest.start_date}
                                - name: END_DATE
                                  value: $${manifest.end_date}
                                - name: DRY_RUN
                                  value: $${dry_run}
                                - name: SPLIT_BY
                                  value: day
                                - name: WORKFLOW_NAME
                                  value: $${workflow_name}
                    result: pv_extract_result

                - wait_for_pv_extract:
                    call: await_job
                    args:
                      execution_name: $${pv_extract_result.name}

        - extract_weather:
            parallel:
              for:
                value: pv_system_id
                in: $${pv_system_ids}
                steps:
                  - run_weather_extract:
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
                                    value: $${weather_data_source}
                                  - name: PV_SYSTEM_ID
                                    value: $${string(pv_system_id)}
                                  - name: START_DATE
                                    value: $${manifest.start_date}
                                  - name: END_DATE
                                    value: $${manifest.end_date}
                                  - name: DRY_RUN
                                    value: $${dry_run}
                                  - name: WORKFLOW_NAME
                                    value: $${workflow_name}
                      result: weather_extract_result

                  - wait_for_weather_extract:
                      call: await_job
                      args:
                        execution_name: $${weather_extract_result.name}

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
            result: commit_result

        - wait_for_commit:
            call: await_job
            args:
              execution_name: $${commit_result.name}

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

        - wait_for_consolidate:
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
              - condition: $${map.get(exec, "terminalCondition") == null}
                steps:
                  - wait:
                      call: sys.sleep
                      args:
                        seconds: 10
                  - retry:
                      next: check_status
              - condition: $${exec.terminalCondition.state == "SUCCEEDED"}
                return: $${exec}
              - condition: true
                raise:
                  message: $${"Job execution failed with state " + exec.terminalCondition.state}
                  data: $${exec}
  YAML
}
