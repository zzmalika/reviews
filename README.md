# reviews

## Quickstart


### 1. Venv

```bash
python3 -m venv reviews_venv
source ibit_venv/bin/activate
```
### 2. Docker

```bash
docker-compose build
docker-compose up -d
```

### 3. Open airflow

```bash
localhost:8080
```

### TODO

``bash
- Extract all_reviews using batch method
- Save batches to parquet
- Upload all data to clickhouse as raw layer and then transform in clickhouse?
- Parallel tasks for en and ru, increase number of workers 
- What if number of reviews for event_date is 0?
- logging
- configs, envs
```