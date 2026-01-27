# Storage Module

This module manages Google Cloud Storage resources for DVC and data management.

## Resources Created

- **GCS Bucket**: Storage bucket with hierarchical namespace enabled
- **Service Account**: DVC service account for authentication
- **IAM Binding**: Grants the service account object creator role on the bucket

## Variables

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `bucket_name` | string | - | Name of the GCS bucket for data storage |
| `region` | string | `europe-west2` | GCP region for the storage bucket |

## Outputs

| Name | Description |
|------|-------------|
| `service_account_email` | Service account email for DVC authentication |
| `bucket_name` | Name of the created storage bucket |
| `bucket_url` | URL of the storage bucket |

## Usage

```hcl
module "storage" {
  source      = "./modules/storage"
  bucket_name = "my-data-bucket"
  region      = "europe-west2"
}
```

## Example

```hcl
module "storage" {
  source      = "./modules/storage"
  bucket_name = "pv-prospect-data"
  region      = "europe-west2"
}

output "dvc_sa" {
  value = module.storage.service_account_email
}
```

