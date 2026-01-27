# Main Terraform configuration
# Provider and general settings

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.12.0"
    }
  }
}

provider "google" {
  project = var.project_id
}

# Storage module for DVC and data management
module "storage" {
  source      = "./modules/storage"
  bucket_name = var.bucket_name
  region      = var.region
}

# Kafka cluster module
module "kafka" {
  source = "./modules/kafka"
  region = var.region
  zone   = var.zone
}

# Outputs from modules
output "service_account_email" {
  value       = module.storage.service_account_email
  description = "Use this service account for application authentication."
}

output "bucket_name" {
  value       = module.storage.bucket_name
  description = "Name of the created storage bucket"
}

output "zookeeper_internal_ip" {
  value       = module.kafka.zookeeper_internal_ip
  description = "Internal IP of Zookeeper instance"
}

output "zookeeper_external_ip" {
  value       = module.kafka.zookeeper_external_ip
  description = "External IP of Zookeeper instance"
}

output "kafka_broker_internal_ips" {
  value       = module.kafka.kafka_broker_internal_ips
  description = "Internal IPs of Kafka broker instances"
}

output "kafka_broker_external_ips" {
  value       = module.kafka.kafka_broker_external_ips
  description = "External IPs of Kafka broker instances"
}

output "kafka_bootstrap_servers" {
  value       = module.kafka.kafka_bootstrap_servers
  description = "Kafka bootstrap servers connection string (internal IPs)"
}


