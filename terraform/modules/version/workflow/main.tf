# Cloud Workflows orchestration for data versioning and model training.
#
# Step 1: run the data-versioner Cloud Run Job to create a data-v<date> tag.
# Step 2: run the model-trainer Cloud Run Job to train and (if gate passes)
#         promote a new model keyed to that same data snapshot.
#
# The versioner has a known hang-on-exit bug (see briefs/versioner-hang.md):
# it completes all work in ~90s but its container then stalls until the 30-min
# Cloud Run timeout kills it, causing Cloud Run to report Completed=False.
# Step 1 therefore uses try/except and proceeds to Step 2 regardless — the
# trainer self-verifies by cloning the instance repo at the data-v<date> tag,
# so if the versioner genuinely failed (no tag), the trainer fails cleanly.

resource "google_workflows_workflow" "data_versioning" {
  name            = "pv-prospect-version"
  region          = var.region
  service_account = var.service_account_email
  call_log_level  = "LOG_ERRORS_ONLY"
  description     = "Runs the data versioner then the model trainer"

  source_contents = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - project_id: $${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
              - region: "${var.region}"
              - versioner_job: "${var.cloud_run_job_name}"
              - trainer_job: "${var.trainer_job_name}"
              - version_date: $${default(map.get(args, "version_date"), text.substring(time.format(sys.now()), 0, 10))}

        - run_versioner:
            try:
              steps:
                - exec_versioner:
                    call: googleapis.run.v2.projects.locations.jobs.run
                    args:
                      name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + versioner_job}
                      body:
                        overrides:
                          containerOverrides:
                            - env:
                                - name: VERSION_DATE
                                  value: $${version_date}
                    result: version_result
            except:
              as: e
              steps:
                - log_versioner_non_success:
                    call: sys.log
                    args:
                      text: $${"Versioner reported non-success (may be hang bug — see briefs/versioner-hang.md); proceeding to trainer which will self-verify: " + json.encode_to_string(e)}
                      severity: WARNING

        - run_trainer:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + trainer_job}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: DATA_VERSION
                          value: $${version_date}
            result: trainer_result

        - done:
            return: $${trainer_result}
  YAML
}
