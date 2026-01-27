output "zookeeper_internal_ip" {
  value       = google_compute_instance.zookeeper.network_interface[0].network_ip
  description = "Internal IP of Zookeeper instance"
}

output "zookeeper_external_ip" {
  value       = google_compute_instance.zookeeper.network_interface[0].access_config[0].nat_ip
  description = "External IP of Zookeeper instance"
}

output "kafka_broker_internal_ips" {
  value       = [for instance in google_compute_instance.kafka_broker : instance.network_interface[0].network_ip]
  description = "Internal IPs of Kafka broker instances"
}

output "kafka_broker_external_ips" {
  value       = [for instance in google_compute_instance.kafka_broker : instance.network_interface[0].access_config[0].nat_ip]
  description = "External IPs of Kafka broker instances"
}

output "kafka_bootstrap_servers" {
  value       = join(",", [for instance in google_compute_instance.kafka_broker : "${instance.network_interface[0].network_ip}:9092"])
  description = "Kafka bootstrap servers connection string (internal IPs)"
}

output "network_name" {
  value       = google_compute_network.kafka_network.name
  description = "Name of the Kafka VPC network"
}

output "subnet_name" {
  value       = google_compute_subnetwork.kafka_subnet.name
  description = "Name of the Kafka subnet"
}

