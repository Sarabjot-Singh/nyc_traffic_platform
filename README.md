# NYC Taxi Platform

This project builds a small data platform for ingesting NYC taxi trip data, storing it in a bronze layer, and transforming it into silver-layer models for analytics.

## Architecture Overview

The pipeline is organized around three layers:

- Bronze: downloads NYC taxi parquet files and stores them in object storage.
- Silver: reads the bronze data, curates it, and builds Delta-based models.
- Shared utilities: provide logging, Spark session creation, database access, and environment handling.

The main flow is:

1. Ingest data from the NYC Taxi public source.
2. Store the raw parquet files in the configured storage bucket.
3. Load and transform the data into curated silver datasets.
4. Write the results to Delta tables for downstream use.

## Project Structure

- [config.yaml](config.yaml) — central configuration for storage paths, dataset settings, and layer names.
- [docker-compose.yml](docker-compose.yml) — local services for PostgreSQL, Spark, and MinIO.
- [Makefile](Makefile) — common commands to build and run the local stack.
- [src/bronze/yellow_trips/extract_load.py](src/bronze/yellow_trips/extract_load.py) — bronze ingestion script for NYC yellow taxi data.
- [src/bronze/yellow_trips/schema.py](src/bronze/yellow_trips/schema.py) — schema definition used during bronze ingestion.
- [src/silver/yellow_trips/fact_yellow_trip.py](src/silver/yellow_trips/fact_yellow_trip.py) — silver fact table build logic for yellow taxi data.
- [src/silver/base.py](src/silver/base.py) — base class for silver model implementations.
- [src/common/spark.py](src/common/spark.py) — shared Spark session manager.
- [src/common/database.py](src/common/database.py) — shared PostgreSQL access helper.
- [src/common/logger.py](src/common/logger.py) — shared logging setup.
- [src/common/favicon.py](src/common/favicon.py) — emoji-based logging markers.

## Setup

### 1. Prerequisites

Make sure you have:

- Python 3.14+
- Docker Desktop
- uv (recommended) or pip

### 2. Environment Variables

Copy the example environment file and fill in your values:

### 3. Install Dependencies

Using uv:

```bash
uv sync
```

Or with pip:

```bash
pip install -r requirements.txt
```

### 4. Start Local Infrastructure

make build
This Creates the docker Images in a Container

```bash
make up
```

This starts:

- PostgreSQL
- Spark
- MinIO

### 5. Run the Pipeline

Run the bronze ingestion job:

```bash
uv run python src/bronze/yellow_trips/extract_load.py
```

Run the silver transformation job:

```bash
uv run python src/silver/yellow_trips/fact_yellow_trip.py
```

## How the Files Work Together

- [config.yaml](config.yaml) contains the main dataset and storage settings.
- [src/bronze/yellow_trips/extract_load.py](src/bronze/yellow_trips/extract_load.py) uses the config and schema to download NYC taxi parquet files and write them to the bronze storage path.
- [src/bronze/yellow_trips/schema.py](src/bronze/yellow_trips/schema.py) defines the expected column types used during bronze ingestion.
- [src/common/spark.py](src/common/spark.py) and [src/common/database.py](src/common/database.py) are shared by the ingestion and transformation scripts so they can access Spark and PostgreSQL consistently.
- [src/silver/yellow_trips/fact_yellow_trip.py](src/silver/yellow_trips/fact_yellow_trip.py) consumes the curated bronze data, joins the relevant dimensions, and writes the resulting fact table as a Delta dataset.
- [src/silver/base.py](src/silver/base.py) defines a reusable interface for silver models.

## Notes

- The project currently focuses on yellow taxi data, but the structure is designed to support additional datasets later.
- Logging is centralized through [src/common/logger.py](src/common/logger.py) and enriched with emoji markers from [src/common/favicon.py](src/common/favicon.py).
