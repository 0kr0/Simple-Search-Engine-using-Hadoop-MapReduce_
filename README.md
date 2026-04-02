# big-data-assignment2

# How to run
## Step 1: Install prerequisites
- Docker
- Docker Compose

## Step 2: Data
Place your Wikipedia parquet export as `app/data/a.parquet` (path expected by `app/prepare_data.sh`). Parquet files are gitignored.

## Step 3: Run the stack
From the repository root:
```bash
docker compose up
```
This starts three containers: Hadoop/Spark master, one worker, and Cassandra. The master runs `app/app.sh`, which prepares data on HDFS, builds the MapReduce index, loads Cassandra, and runs a sample search.

To search again inside the master container (after the stack is up and indexing finished):
```bash
docker exec -it cluster-master bash -lc 'cd /app && bash search.sh your query words'
```

### Environment (optional)
- `PREPARE_NUM_DOCS` — sample size (default `1000`) for `prepare_data.py`, set before `docker compose` if you inject a custom `app.sh` or run scripts manually.
- `INDEX_INPUT`, `INDEX_INVERTED`, `INDEX_DOCS` — HDFS paths for indexing and `app.py` (see `app/create_index.sh` and `app/app.py`).
