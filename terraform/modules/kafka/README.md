# Kafka Module

This module deploys a complete Apache Kafka cluster on Google Cloud Platform.

## Resources Created

- **VPC Network**: Dedicated network for Kafka cluster isolation
- **Subnet**: Custom subnet with configurable CIDR range
- **Firewall Rules**: 
  - Internal communication (Kafka: 9092, 9093; Zookeeper: 2181, 2888, 3888)
  - SSH access for management
- **Zookeeper Instance**: Single instance for cluster coordination
- **Kafka Brokers**: Multiple broker instances (default: 2)

## Architecture

```
VPC Network (kafka-network)
├── Subnet (configurable CIDR)
│   ├── Zookeeper (1 instance)
│   └── Kafka Brokers (configurable count)
└── Firewall Rules
    ├── Internal traffic
    └── SSH access
```

## Variables

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `region` | string | (required) | GCP region for Kafka cluster |
| `zone` | string | (required) | GCP zone for Kafka cluster |
| `kafka_broker_count` | number | `2` | Number of Kafka broker instances |
| `kafka_machine_type` | string | `e2-small` | Machine type for Kafka brokers |
| `zookeeper_machine_type` | string | `e2-micro` | Machine type for Zookeeper instances |
| `network_cidr` | string | `10.0.0.0/24` | CIDR range for Kafka subnet |

## Outputs

| Name | Description |
|------|-------------|
| `zookeeper_internal_ip` | Internal IP of Zookeeper instance |
| `zookeeper_external_ip` | External IP of Zookeeper instance |
| `kafka_broker_internal_ips` | List of internal IPs for all brokers |
| `kafka_broker_external_ips` | List of external IPs for all brokers |
| `kafka_bootstrap_servers` | Kafka bootstrap servers connection string |
| `network_name` | Name of the VPC network |
| `subnet_name` | Name of the subnet |

## Usage

```hcl
module "kafka" {
  source                   = "./modules/kafka"
  region                   = "europe-west2"
  zone                     = "europe-west2-a"
  kafka_broker_count       = 2
  kafka_machine_type       = "e2-small"
  zookeeper_machine_type   = "e2-micro"
  network_cidr             = "10.0.0.0/24"
}
```

## Configuration Details

### Kafka Version
- Apache Kafka 3.6.1 (kafka_2.13-3.6.1)

### Kafka Configuration
- Default partitions: 3
- Replication factor: 1 (increase for production)
- Log retention: 168 hours (7 days)
- Max message size: 100MB

### System Services
Both Zookeeper and Kafka are configured as systemd services with:
- Automatic startup on boot
- Automatic restart on failure

## Testing

After deployment, SSH to a broker and test:

```bash
# SSH to broker
gcloud compute ssh kafka-broker-0 --zone=europe-west2-a

# Create topic
/opt/kafka/bin/kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 3

# List topics
/opt/kafka/bin/kafka-topics.sh --list \
  --bootstrap-server localhost:9092
```

## Security Notes

⚠️ **Production Considerations**:
- This module creates instances with public IPs for SSH access
- SSH is open to 0.0.0.0/0 (restrict for production)
- No SSL/TLS or SASL authentication configured
- Consider using Cloud NAT and private IPs for production
- Enable Cloud Monitoring and Logging

## Scaling

To scale the cluster, modify the `kafka_broker_count` variable and apply:

```hcl
module "kafka" {
  source             = "./modules/kafka"
  kafka_broker_count = 5  # Scale to 5 brokers
  # ...other variables
}
```

