variable "region" {
  type        = string
  description = "GCP region for Kafka cluster"
}

variable "zone" {
  type        = string
  description = "GCP zone for Kafka cluster"
}

variable "kafka_broker_count" {
  type        = number
  default     = 2
  description = "Number of Kafka broker instances"
}

variable "kafka_machine_type" {
  type        = string
  default     = "e2-small"
  description = "Machine type for Kafka brokers"
}

variable "zookeeper_machine_type" {
  type        = string
  default     = "e2-micro"
  description = "Machine type for Zookeeper instances"
}

variable "network_cidr" {
  type        = string
  default     = "10.0.0.0/24"
  description = "CIDR range for Kafka subnet"
}

