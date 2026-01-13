# PV Prospect Data Extraction Pipeline

Data extraction pipeline for PV Prospect.

## Running the Project

### Prerequisites

- Docker and Docker Compose installed
- Required credentials files in `pv_prospect/data_extraction/resources/`:
  - `gdrive_credentials.json`
  - `gdrive_token.json`

### Starting the Infrastructure

Start the RabbitMQ broker, Celery worker, and Flower monitoring UI:

```bash
docker compose up -d rabbitmq worker flower
```

This starts:
- **RabbitMQ**: Message broker for task queuing (management UI at http://localhost:15672)
- **Worker**: Celery worker that processes extraction tasks
- **Flower**: Task monitoring UI at http://localhost:5555

### Running the Task Producer

Use `docker compose run` to execute the task producer with custom arguments while staying on the Docker network:

```bash
docker compose run --rm taskproducer <source> <system_ids> [options]
```

The `--rm` flag automatically removes the container after execution.

#### Examples

**Single system, single day:**
```bash
docker compose run --rm taskproducer weather-om-60 89665 -d 2024-01-15 -x -l temp
```

**Multiple systems (comma-separated):**
```bash
docker compose run --rm taskproducer weather-om-60 89665,12345,67890 -d 2024-01-15 -x -l temp
```

**Multiple sources (comma-separated):**
```bash
docker compose run --rm taskproducer weather-om-60,weather-om-15 89665 -d 2024-01-15 -x -l temp
```

**Process entire month (YYYY-MM format):**
```bash
docker compose run --rm taskproducer weather-om-60 89665 -d 2024-01 -x -l temp
```

**Custom date range:**
```bash
docker compose run --rm taskproducer weather-om-60 89665 -d 2024-01-01 -e 2024-01-31 -x -l temp
```

**Process all systems (omit system_ids):**
```bash
docker compose run --rm taskproducer weather-om-60 -d 2024-01-15 -x -l temp
```

**Process by week instead of by day:**
```bash
docker compose run --rm taskproducer weather-om-60 89665 -d 2024-01-01 -e 2024-01-31 -w -x -l temp
```

**Dry run (preview without executing):**
```bash
docker compose run --rm taskproducer weather-om-60 89665 -d 2024-01-15 -x -l temp -n
```

### Option 2 — Run task producer from host but use dockerised broker & worker (recommended for local dev)

If you want to run the task producer from your host machine (so you can iterate quickly and pass CLI args directly) but still use the RabbitMQ broker and Celery worker running inside Docker, run the producer container via `docker compose run` and bind-mount a host directory to the service so any files written with `-l/--local-dir` are visible on the host.

How this works:
- `docker compose run` creates a short-lived container attached to the compose network so it can reach `rabbitmq:5672` and the worker.
- Pass all task producer CLI arguments after the service name; they are forwarded into the container entrypoint.
- Use `-v` to bind-mount a host directory into the container at the path you will pass to `-l` so any files the container creates are immediately visible on the host.

Example (bind a host folder `./out` to `/data/out` inside the container and pass `-l /data/out`):

```bash
mkdir -p out
# Run taskproducer in the compose network, mount ./out into the container at /data/out
docker compose run --rm -v "$(pwd)/out:/data/out" taskproducer weather-om-60 89665 -d 2024-01-15 -x -l /data/out
```

Notes and tips:
- Do NOT expose the AMQP port (5672) on the host — the container will reach the broker over the compose network at `rabbitmq:5672` as configured in `docker-compose.yml`.
- The path you pass to `-l/--local-dir` must be the path inside the container (e.g. `/data/out` in the example). Use an absolute path inside the container to avoid ambiguity.
- You can reuse the same bind mount for multiple runs; files written by the taskproducer will appear under `./out` on your host.
- If you mount a directory that is not empty, the container will use it as the base for writes; permissions are determined by Docker and host user mappings — if you see permission errors, try adjusting ownership or use `--user $(id -u):$(id -g)` on the `docker compose run` command.

This option keeps the messaging entirely on the Docker network while letting you run the producer with local credentials and inspect output files on your host.

### Command-Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `source` | Data source(s) to extract (comma-separated) | `weather-om-60` or `weather-om-60,weather-om-15` |
| `system_ids` | PV system ID(s) to process (comma-separated, optional) | `89665` or `89665,12345` |
| `-d, --start-date` | Start date: YYYY-MM-DD, YYYY-MM, 'today', or 'yesterday' (default: yesterday) | `-d 2024-01-15` |
| `-e, --end-date` | End date: YYYY-MM-DD, YYYY-MM, 'today', or 'yesterday' (default: start date + 1 day) | `-e 2024-01-31` |
| `-l, --local-dir` | Save to local directory instead of Google Drive | `-l temp` |
| `-x, --write-metadata` | Write extractor metadata as JSON alongside CSV files | `-x` |
| `-o, --overwrite` | Overwrite existing files (default: skip existing) | `-o` |
| `-w, --by-week` | Process one week at a time instead of day-by-day | `-w` |
| `-r, --reverse` | Process dates in reverse chronological order | `-r` |
| `-n, --dry-run` | Show what would be done without executing | `-n` |

### Available Data Sources

- `pv` - PVOutput data
- `weather-om-15` - OpenMeteo 15-minute data
- `weather-om-60` - OpenMeteo hourly data
- `weather-om-satellite` - OpenMeteo satellite data
- `weather-om-historical` - OpenMeteo historical data
- `weather-om-15-v0` - OpenMeteo v0 15-minute data
- `weather-om-60-v0` - OpenMeteo v0 hourly data
- `weather-vc-15` - Visual Crossing 15-minute data
- `weather-vc-60` - Visual Crossing hourly data

### Monitoring Tasks

- **Flower UI**: http://localhost:5555 - Real-time task monitoring, worker status, task history
- **RabbitMQ Management**: http://localhost:15672 - Queue statistics, message rates (login: user/password)

### Stopping the Services

Stop and remove all containers:
```bash
docker compose down
```

Stop containers but keep them for later:
```bash
docker compose stop
```

### Troubleshooting

**View worker logs:**
```bash
docker compose logs worker
```

**View task producer logs:**
```bash
docker compose logs taskproducer
```

**Check running services:**
```bash
docker compose ps
```

**Restart a service:**
```bash
docker compose restart worker
```
