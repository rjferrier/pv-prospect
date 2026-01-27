# Kafka Cluster Infrastructure

# VPC Network for Kafka cluster
resource "google_compute_network" "kafka_network" {
  name                    = "kafka-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "kafka_subnet" {
  name          = "kafka-subnet"
  ip_cidr_range = var.network_cidr
  region        = var.region
  network       = google_compute_network.kafka_network.id
}

# Firewall rules for Kafka cluster
resource "google_compute_firewall" "kafka_internal" {
  name    = "kafka-internal"
  network = google_compute_network.kafka_network.name

  allow {
    protocol = "tcp"
    ports    = ["9092", "9093", "2181", "2888", "3888"]
  }

  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["kafka-broker", "zookeeper"]
}

resource "google_compute_firewall" "kafka_ssh" {
  name    = "kafka-ssh"
  network = google_compute_network.kafka_network.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["kafka-broker", "zookeeper"]
}

# Zookeeper instance
resource "google_compute_instance" "zookeeper" {
  name         = "kafka-zookeeper"
  machine_type = var.zookeeper_machine_type
  zone         = var.zone

  tags = ["zookeeper"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 20
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kafka_subnet.id

    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y openjdk-11-jdk wget

    # Download and install Zookeeper (part of Kafka)
    cd /opt
    wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
    tar -xzf kafka_2.13-3.6.1.tgz
    ln -s kafka_2.13-3.6.1 kafka

    # Configure Zookeeper
    cat > /opt/kafka/config/zookeeper.properties <<ZOOEOF
dataDir=/var/lib/zookeeper
clientPort=2181
maxClientCnxns=0
admin.enableServer=false
ZOOEOF

    mkdir -p /var/lib/zookeeper

    # Create systemd service for Zookeeper
    cat > /etc/systemd/system/zookeeper.service <<SVCEOF
[Unit]
Description=Apache Zookeeper
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
SVCEOF

    systemctl daemon-reload
    systemctl enable zookeeper
    systemctl start zookeeper
  EOF

  service_account {
    scopes = ["cloud-platform"]
  }
}

# Kafka broker instances
resource "google_compute_instance" "kafka_broker" {
  count        = var.kafka_broker_count
  name         = "kafka-broker-${count.index}"
  machine_type = var.kafka_machine_type
  zone         = var.zone

  tags = ["kafka-broker"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 50
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.kafka_subnet.id

    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update
    apt-get install -y openjdk-11-jdk wget

    # Download and install Kafka
    cd /opt
    wget https://downloads.apache.org/kafka/3.6.1/kafka_2.13-3.6.1.tgz
    tar -xzf kafka_2.13-3.6.1.tgz
    ln -s kafka_2.13-3.6.1 kafka

    # Wait for Zookeeper to be ready
    sleep 30

    # Configure Kafka
    ZOOKEEPER_IP="${google_compute_instance.zookeeper.network_interface[0].network_ip}"
    BROKER_ID=${count.index}

    cat > /opt/kafka/config/server.properties <<KAFKAEOF
broker.id=$BROKER_ID
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://$(hostname -I | awk '{print $1}'):9092
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/var/lib/kafka-logs
num.partitions=3
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=$ZOOKEEPER_IP:2181
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
KAFKAEOF

    mkdir -p /var/lib/kafka-logs

    # Create systemd service for Kafka
    cat > /etc/systemd/system/kafka.service <<SVCEOF
[Unit]
Description=Apache Kafka
After=network.target

[Service]
Type=simple
User=root
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
SVCEOF

    systemctl daemon-reload
    systemctl enable kafka
    systemctl start kafka
  EOF

  service_account {
    scopes = ["cloud-platform"]
  }

  depends_on = [google_compute_instance.zookeeper]
}


