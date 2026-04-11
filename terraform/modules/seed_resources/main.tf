# Uploads static resource files to the staging bucket's resources/ prefix.
#
# These files must exist on GCS before the extraction pipeline runs.
# Currently this covers the UK weather grid point sample files used by
# the weather grid backfill workflow.

resource "google_storage_bucket_object" "point_samples" {
  for_each = fileset(var.point_samples_dir, "sample_*.csv")

  bucket = var.staging_bucket_name
  name   = "resources/point_samples/${each.value}"
  source = "${var.point_samples_dir}/${each.value}"
}
