variable "staging_bucket_name" {
  type        = string
  description = "Name of the staging GCS bucket"
}

variable "point_samples_dir" {
  type        = string
  description = "Local directory containing point sample CSV files"
}
