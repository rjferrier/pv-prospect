# Cloud Monitoring: alert policies for model degradation and serving health.
#
# Offline degradation (model trainer):
#   1. PV clamped-power R² below absolute floor (0.70) on any training run.
#   2. Model trainer ran but the new model was rejected by the promotion gate.
#
# Serving health (pv-prospect-app Cloud Run Service):
#   3. 5xx error rate above 1 req/s over 5 min.
#   4. p95 request latency above 30 s over 5 min.

locals {
  notification_channels = (
    var.alert_notification_email != ""
    ? [google_monitoring_notification_channel.email[0].name]
    : []
  )
}

resource "google_monitoring_notification_channel" "email" {
  count        = var.alert_notification_email != "" ? 1 : 0
  display_name = "PV Prospect alerts"
  type         = "email"
  labels = {
    email_address = var.alert_notification_email
  }
}

# ---------------------------------------------------------------------------
# Offline degradation: absolute R² floor
# ---------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "pv_r2_floor" {
  display_name          = "PV Prospect: PV model R² below floor"
  combiner              = "OR"
  notification_channels = local.notification_channels

  conditions {
    display_name = "PV clamped-power R² dropped below 0.70"
    condition_threshold {
      filter          = "metric.type=\"custom.googleapis.com/pv_prospect/pv_clamped_power_r2\" AND resource.type=\"global\""
      comparison      = "COMPARISON_LT"
      threshold_value = 0.70
      duration        = "0s"
      aggregations {
        alignment_period   = "86400s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  alert_strategy {
    auto_close = "604800s"
  }

  documentation {
    content   = "The PV model's clamped-power test R² fell below 0.70 on a scheduled retraining run. Review the model trainer logs and the recent prepared corpus quality."
    mime_type = "text/markdown"
  }
}

# ---------------------------------------------------------------------------
# Offline degradation: promotion gate rejected the new model
# ---------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "model_rejected" {
  display_name          = "PV Prospect: model trainer rejected new model"
  combiner              = "OR"
  notification_channels = local.notification_channels

  conditions {
    display_name = "New model was rejected by the promotion gate"
    condition_threshold {
      filter          = "metric.type=\"custom.googleapis.com/pv_prospect/model_promoted\" AND resource.type=\"global\""
      comparison      = "COMPARISON_LT"
      threshold_value = 1
      duration        = "0s"
      aggregations {
        alignment_period   = "86400s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  alert_strategy {
    auto_close = "604800s"
  }

  documentation {
    content   = "The model trainer completed a retraining run but the new model's R² degraded beyond the configured tolerance. The incumbent model continues serving. Check trainer logs for the new vs incumbent metrics."
    mime_type = "text/markdown"
  }
}

# ---------------------------------------------------------------------------
# Serving health: 5xx error rate
# ---------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "app_5xx_rate" {
  display_name          = "PV Prospect app: elevated 5xx error rate"
  combiner              = "OR"
  notification_channels = local.notification_channels

  conditions {
    display_name = "5xx request rate > 1 req/s over 5 min"
    condition_threshold {
      filter          = "metric.type=\"run.googleapis.com/request_count\" AND resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"pv-prospect-app\" AND metric.labels.response_code_class=\"5xx\""
      comparison      = "COMPARISON_GT"
      threshold_value = 1
      duration        = "300s"
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.labels.service_name"]
      }
    }
  }

  alert_strategy {
    auto_close = "86400s"
  }

  documentation {
    content   = "The pv-prospect-app Cloud Run Service is returning more than 1 5xx response per second. Check Cloud Run logs for error details."
    mime_type = "text/markdown"
  }
}

# ---------------------------------------------------------------------------
# Serving health: p95 latency
# ---------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "app_latency_p95" {
  display_name          = "PV Prospect app: p95 request latency high"
  combiner              = "OR"
  notification_channels = local.notification_channels

  conditions {
    display_name = "p95 latency > 30 s over 5 min"
    condition_threshold {
      filter          = "metric.type=\"run.googleapis.com/request_latencies\" AND resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"pv-prospect-app\""
      comparison      = "COMPARISON_GT"
      threshold_value = 30000
      duration        = "300s"
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_PERCENTILE_95"
      }
    }
  }

  alert_strategy {
    auto_close = "86400s"
  }

  documentation {
    content   = "The pv-prospect-app Cloud Run Service p95 latency exceeded 30 s. With min_instances=0 this may be a cold-start spike; if sustained, check for model-load regressions or scaling issues."
    mime_type = "text/markdown"
  }
}
