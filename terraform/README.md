# PV Prospect Terraform Infrastructure

This repository contains Terraform configurations for deploying the PV Prospect infrastructure on Google Cloud Platform, including storage and Apache Kafka cluster.

## Architecture Overview

```
PV Prospect Infrastructure
├── Storage Module
│   ├── GCS Bucket (hierarchical namespace)
│   ├── DVC Service Account
│   └── IAM Bindings
└── Kafka Module
    ├── VPC Network
    ├── Firewall Rules
    ├── Zookeeper (1x e2-micro instance)
    └── Kafka Brokers (2x e2-small instances)
```

## Directory Structure

```
terraform/
├── main.tf                    # Root module configuration
├── variables.tf               # Root module variables
├── terraform.tfvars           # Variable values (customize this)
├── README.md                  # This file
└── modules/
    ├── storage/               # Storage module
    │   ├── main.tf
    │   ├── variables.tf
    │   ├── outputs.tf
    │   └── README.md
    └── kafka/                 # Kafka cluster module
        ├── main.tf
        ├── variables.tf
        ├── outputs.tf
        └── README.md
```

## Prerequisites

1. **Google Cloud Platform**:
   - Active GCP project
   - Billing enabled
   - Required APIs enabled:
     - Compute Engine API
     - Cloud Storage API
     - IAM API

2. **Terraform**:
   - Terraform >= 1.0
   - Google Cloud provider ~> 6.12.0

3. **Authentication**:
   ```bash
   gcloud auth application-default login
   ```

## Quick Start

### 1. Configure Variables

Create or update `terraform.tfvars`:

```hcl
project_id  = "your-gcp-project-id"
bucket_name = "your-unique-bucket-name"
region      = "europe-west2"
zone        = "europe-west2-a"
```

### 2. Initialize Terraform

```bash
terraform init
```

### 3. Plan Deployment

```bash
terraform plan
```

### 4. Deploy Infrastructure

```bash
terraform apply
```

### 5. Get Outputs

```bash
# Get all outputs
terraform output

# Get specific output
terraform output kafka_bootstrap_servers
terraform output service_account_email
```

## Variables

### Root Module Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `project_id` | string | - | **Required**. GCP project ID |
| `bucket_name` | string | `pv-prospect-data` | Name for the storage bucket |
| `region` | string | `europe-west2` | GCP region for all resources |
| `zone` | string | `europe-west2-a` | GCP zone for compute resources |

**Note**: Kafka-specific variables (broker count, machine types, network CIDR) are configured in the Kafka module with sensible defaults. See [modules/kafka/README.md](modules/kafka/README.md) for customization options.

## Outputs

| Output | Description |
|--------|-------------|
| `service_account_email` | DVC service account email |
| `bucket_name` | Name of the storage bucket |
| `zookeeper_internal_ip` | Zookeeper internal IP |
| `zookeeper_external_ip` | Zookeeper external IP |
| `kafka_broker_internal_ips` | List of Kafka broker internal IPs |
| `kafka_broker_external_ips` | List of Kafka broker external IPs |
| `kafka_bootstrap_servers` | Kafka connection string |

## Modules

### Storage Module

Manages GCS bucket and service accounts for data storage and DVC.

**Location**: `./modules/storage`

See [modules/storage/README.md](modules/storage/README.md) for details.

### Kafka Module

Deploys a complete Apache Kafka cluster with Zookeeper.

**Location**: `./modules/kafka`

**Instance Sizes**: 
- Zookeeper: e2-micro (optimized for cost)
- Kafka Brokers: e2-small (optimized for cost)

See [modules/kafka/README.md](modules/kafka/README.md) for comprehensive documentation.

## Usage Examples

### Deploy Everything

```bash
terraform apply
```

### Deploy Only Storage Module

```bash
terraform apply -target=module.storage
```

### Deploy Only Kafka Module

```bash
terraform apply -target=module.kafka
```

### Scale Kafka Cluster

Update `terraform.tfvars`:
```hcl
kafka_broker_count = 5
```

Then apply:
```bash
terraform apply
```

### Destroy Everything

```bash
terraform destroy
```

### Destroy Only Kafka Cluster

```bash
terraform destroy -target=module.kafka
```

## Testing Kafka Cluster

### Connect to a Broker

```bash
# Get broker IP
BROKER_IP=$(terraform output -json kafka_broker_external_ips | jq -r '.[0]')

# SSH to broker
gcloud compute ssh kafka-broker-0 --zone=europe-west2-a
```

### Create a Test Topic

```bash
/opt/kafka/bin/kafka-topics.sh --create \
  --topic test-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 3
```

### Produce Messages

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic test-topic \
  --bootstrap-server localhost:9092
```

### Consume Messages

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --topic test-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
```

## Cost Estimation

Approximate monthly costs in europe-west2:

| Resource | Specification | Monthly Cost |
|----------|--------------|--------------|
| Zookeeper | 1x e2-micro | ~$7 |
| Kafka Brokers | 2x e2-small | ~$30 |
| GCS Bucket | Storage only | Variable |
| Network Egress | - | Variable |
| **Total** | - | **~$37 + usage** |

> **Note**: Costs are approximate and based on standard pricing. Actual costs may vary based on usage, sustained use discounts, and regional pricing. Instance sizes have been optimized for development/testing environments.

## Security Considerations

⚠️ **Important for Production**:

1. **Networking**:
   - Use Cloud NAT instead of public IPs
   - Restrict SSH access to specific IP ranges
   - Consider VPC peering or VPN for client access

2. **Kafka Security**:
   - Enable SSL/TLS encryption
   - Configure SASL authentication
   - Increase replication factor to 3
   - Enable ACLs for topic access control

3. **Storage**:
   - Enable encryption at rest (already enabled by default)
   - Configure bucket lifecycle policies
   - Set up proper IAM roles and permissions

4. **Monitoring**:
   - Enable Cloud Monitoring
   - Set up Cloud Logging
   - Configure alerts for system health

## Troubleshooting

### Check Terraform State

```bash
terraform show
terraform state list
```

### Validate Configuration

```bash
terraform validate
terraform fmt -check -recursive
```

### Check Zookeeper Status

```bash
gcloud compute ssh kafka-zookeeper --zone=europe-west2-a
systemctl status zookeeper
journalctl -u zookeeper -f
```

### Check Kafka Broker Status

```bash
gcloud compute ssh kafka-broker-0 --zone=europe-west2-a
systemctl status kafka
journalctl -u kafka -f
```

### View Kafka Logs

```bash
tail -f /opt/kafka/bin/../logs/server.log
```

## Maintenance

### Updating Kafka Configuration

To update Kafka configuration:

1. Modify the startup script in `modules/kafka/main.tf`
2. Apply changes with `terraform apply`
3. SSH to brokers and restart service if needed:
   ```bash
   sudo systemctl restart kafka
   ```

### Upgrading Kafka Version

Update the Kafka version in the startup scripts in `modules/kafka/main.tf`:

```bash
# Change from
wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz

# To (example)
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
```

## References

- [Terraform Documentation](https://www.terraform.io/docs)
- [Google Cloud Provider Documentation](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [GeeksforGeeks: Setup Kafka on GCP](https://www.geeksforgeeks.org/apache-kafka/how-to-setup-kafka-on-gcp/)

## Support

For issues or questions:
1. Check module READMEs for specific details
2. Review modules/kafka/README.md for Kafka-specific information
3. Validate configuration with `terraform validate`
4. Check GCP quotas and permissions

## License

[Your License Here]

