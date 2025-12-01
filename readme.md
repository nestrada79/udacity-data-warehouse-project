# Sparkify Data Warehouse ETL Project

## Introduction
Sparkify is a music streaming startup that generates large volumes of user activity logs and song metadata. These JSON files are stored in Amazon S3, but analysts need a faster, scalable way to run queries about user engagement, song popularity, listening behavior, and system trends. This project builds a complete cloud data warehouse using Amazon Redshift Serverless and an ETL pipeline that stages raw data from S3 and loads it into a dimensional star schema.

## Project Summary
This project implements:
- Amazon Redshift Serverless as the cloud data warehouse.
- Amazon S3 as the storage location for raw JSON event logs and song metadata.
- A complete ETL process:
  - `create_tables.py` recreates all staging and warehouse tables.
  - `etl.py` loads data from S3 into staging tables, then inserts transformed data into analytics tables.
- A star schema optimized for analytical queries.
- Staging tables that decouple raw JSON structure from the warehouse.

## Dataset
### Song Data
Contains JSON metadata about songs and artists, including:
- Artist ID, name, location
- Song ID, title, year, duration

### Log Data
Contains user activity logs from the Sparkify app, including:
- Timestamps (ts)
- User attributes
- Song and artist text metadata
- Event information (page = "NextSong")

## Schema Design
The project uses a **star schema**, optimized for analytic workloads.

### Fact Table: `songplays`
Contains song play events.

| Column | Description |
|--------|-------------|
| songplay_id | Primary key (IDENTITY) |
| start_time | Timestamp of event |
| user_id | User who listened |
| level | Paid/free level |
| song_id | Song ID from songs table |
| artist_id | Artist ID from artists table |
| session_id | App session |
| location | User location |
| user_agent | Browser/device info |

### Dimension Tables
#### `users`
User profiles and subscription levels.

#### `songs`
Song metadata for analytics.

#### `artists`
Artist identities and metadata.

#### `time`
Breakdown of timestamps into hour, day, week, month, year, weekday.

## Staging Tables
Staging tables store raw JSON data before transformation:
- `staging_events`
- `staging_songs`

They use wide VARCHARs to prevent COPY failures.

## ETL Pipeline
### 1. Run `create_tables.py`
Recreates staging and analytics tables.

```bash
python create_tables.py
```

### 2. Run `etl.py`
Loads JSON from S3 → staging tables → star schema.

```bash
python etl.py
```

Progress is printed for each COPY and INSERT operation.

## File Structure
```
udacity-data-warehouse-project/
├── create_tables.py
├── etl.py
├── sql_queries.py
├── dwh.cfg
├── .env (not committed)
└── README.md
```

## Configuration
Sensitive credentials are stored in `.env` (ignored by Git). Example:
```
AWS_ACCESS_KEY_ID=xxxx
AWS_SECRET_ACCESS_KEY=xxxx
REDSHIFT_HOST=your-redshift-host
REDSHIFT_DB=dev
REDSHIFT_USER=awsuser
REDSHIFT_PASSWORD=xxxx
REDSHIFT_PORT=5439
```

`dwh.cfg` stores non-secret settings:
```
[S3]
LOG_DATA=s3://your-bucket/log-data/
LOG_JSONPATH=s3://your-bucket/log_json_path.json
SONG_DATA=s3://your-bucket/song-data/

[REGION]
NAME=us-east-1
```

## Example Analytics Queries
### Most played songs
```sql
SELECT s.title, COUNT(*) AS plays
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY plays DESC
LIMIT 10;
```

### Peak listening hours
```sql
SELECT hour, COUNT(*) AS plays
FROM time
JOIN songplays ON time.start_time = songplays.start_time
GROUP BY hour
ORDER BY plays DESC;
```

### Top artists
```sql
SELECT a.name, COUNT(*) AS plays
FROM songplays sp
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY a.name
ORDER BY plays DESC
LIMIT 10;
```

## Why This Architecture?
- **Star schema** → fast analytical queries.
- **Staging tables** → isolate raw JSON from final warehouse design.
- **Redshift Serverless** → auto-scaling, fast COPY operations, and parallel processing.
- **S3 → Redshift COPY** → fastest bulk load method for JSON data.
- **Separation of DDL and ETL** → cleaner, testable, and industry-standard.

## Conclusion
This project delivers a complete cloud-based analytics warehouse for Sparkify. The ETL pipeline loads raw JSON data from S3, stages it in Redshift, and transforms it into a schema tailored for analytical insights into user behavior, artist popularity, and song trends.

