variable "bucket_prefix" {
  type        = string
  description = "Prefix for the GCS data buckets"
  default     = "pv-prospect"
}

variable "region" {
  type        = string
  description = "GCP region for the storage buckets"
}
