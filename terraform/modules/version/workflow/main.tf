# Cloud Workflows orchestration for data versioning
#
# A single-step workflow that runs the versioner Cloud Run Job.

resource "google_workflows_workflow" "data_versioning" {
  name            = "pv-prospect-version"
  region          = var.region
  service_account = var.service_account_email
  description     = "Runs the PV Prospect data versioner Cloud Run Job"

  source_contents = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - project_id: $${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
              - region: "${var.region}"
              - job_name: "${var.cloud_run_job_name}"
              - version_date: $${default(map.get(args, "version_date"), text.substring(time.format(sys.now()), 0, 10))}

        - run_versioner:
            call: googleapis.run.v2.projects.locations.jobs.run
            args:
              name: $${"projects/" + project_id + "/locations/" + region + "/jobs/" + job_name}
              body:
                overrides:
                  containerOverrides:
                    - env:
                        - name: VERSION_DATE
                          value: $${version_date}
            result: version_result

        - done:
            return: $${version_result}
  YAML
}
