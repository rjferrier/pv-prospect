# Cloud Workflows orchestration for the daily grid-point weather backfill.
#
# This workflow is manifest-driven:
#
#   1. It invokes the Cloud Run Job with JOB_TYPE=plan_grid_point_backfill,
#      which writes a JSON manifest to GCS describing the day's work.
#   2. It reads the manifest from GCS, then dispatches one extract_and_load
#      Cloud Run Job per batch, sleeping between dispatches to stay under
#      OpenMeteo's per-hour rate limit.
#   3. After all batches succeed, it invokes the Cloud Run Job with
#      JOB_TYPE=commit_grid_point_backfill, which advances the live cursor
#      so that tomorrow's run picks up from the next point in the backfill.
#
# If any batch fails, the workflow aborts without committing the cursor, so
# tomorrow's plan job will re-derive the same manifest and retry.

resource "google_workflows_workflow" "backfill" {
  name            = "pv-prospect-extract-backfill"
  region          = var.region
  service_account = var.service_account_email
  description     = "Orchestrates the daily grid-point weather backfill via a manifest + paced Cloud Run Job dispatch"

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
              - manifest_object: "${var.manifest_object_path}"
              - data_source: $${default(map.get(args, "data_source"), "${var.data_source}")}
              - sleep_seconds: $${default(map.get(args, "sleep_seconds_between_batches"), ${var.sleep_seconds_between_batches})}
              - overwrite: $${default(map.get(args, "overwrite"), "false")}
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
                          value: plan_grid_point_backfill
            result: plan_result

        - fetch_manifest:
            call: googleapis.storage.v1.objects.get
            args:
              bucket: $${bucket}
              object: $${text.url_encode(manifest_object)}
              alt: media
            result: manifest

        - collect_batches:
            assign:
              - batches: $${list.concat([manifest.step2_batch], manifest.step3_batches)}

        - dispatch:
            for:
              value: batch
              index: i
              in: $${batches}
              steps:
                - maybe_sleep:
                    switch:
                      - condition: $${i > 0}
                        steps:
                          - pace:
                              call: sys.sleep
                              args:
                                seconds: $${sleep_seconds}
                - run_batch:
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
                                  value: $${data_source}
                                - name: SAMPLE_FILE_INDEX
                                  value: $${string(batch.sample_file_index)}
                                - name: START_DATE
                                  value: $${batch.start_date}
                                - name: END_DATE
                                  value: $${batch.end_date}
                                - name: OVERWRITE
                                  value: $${overwrite}
                                - name: DRY_RUN
                                  value: $${dry_run}
                    result: batch_result

        - commit:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: JOB_TYPE
                          value: commit_grid_point_backfill
            result: commit_result

        - done:
            return: "completed"
  YAML
}
